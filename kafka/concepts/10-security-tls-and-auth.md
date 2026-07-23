# 보안: 암호화(TLS)와 인증(SASL·mTLS)

Kafka 보안을 "무엇을 막는가" 관점에서 정리하고, PLAINTEXT와 SASL_SSL 중 무엇을 고를지, 서버 인증이 실제로 어떻게 도는지 설명합니다. 리스너 설계는 [06-cluster-design](06-cluster-design.md), 설치는 [07-kraft-cluster-installation](07-kraft-cluster-installation.md), 인증서 생성 절차는 예제의 `certs/README.md`를 참고합니다.

## 1. 보안이 없으면 (PLAINTEXT의 위험)

```text
 Producer ──(주문/결제 데이터, 평문)──► Broker
                    ▲
          네트워크를 엿보면(스위치 미러링, 탭, 잘못 설정된 라우터,
          클라우드 VPC 스니핑) → 카드번호·개인정보·주문내역이 그대로 노출
```

"완전히 격리된 신뢰 망"이 보장되지 않으면 평문은 위험합니다.

## 2. 보안의 두 축 — 암호화 vs 인증

혼동하기 쉬운데, 서로 다른 문제를 풉니다.

| 수단 | 무엇을 하나 | 막는 것 |
| --- | --- | --- |
| **TLS** | 전송 암호화 + 서버 인증 (+ mTLS 상호 인증) | 도청·위장·중간자 |
| **SASL/SCRAM** | 클라이언트 인증(**너는 누구냐**) | 무단 접속 |
| **ACL** | 권한(이 계정은 **무엇을** 할 수 있나) | 권한 초과 |

우리 운영 구성(버전 B)은 **TLS + SASL/SCRAM**을 함께 써서 "엿보기 방지 + 위장 방지 + 접근 통제"를 만듭니다.

## 3. 무엇이 "클라이언트"인가

브로커에 접속해 produce/consume 하는 모든 주체가 클라이언트이고, **가장 대표적인 게 Kafka를 이용하는 API 서버(MSA 서비스)** 입니다.

```text
 order-service(주문 API)      → produce → 프로듀서 클라이언트
 notification-service(알림)   → consume → 컨슈머 클라이언트
 CLI 도구 / Kafka Connect / Admin 도구 도 클라이언트
```

- SASL 서비스 계정(예: `order-service`) = 그 API 서버 클라이언트의 로그인 계정.
- CLIENT 리스너(9094) = 이런 클라이언트가 붙는 포트.
- 클라이언트 설정(`bootstrap.servers`, `acks=all`, truststore 등)은 그 **API 서버 코드/설정**에 들어갑니다.

> "클라이언트"는 문맥에 따라 달라집니다. 브로커 간 통신에선 한 브로커가 다른 브로커의 클라이언트가 되고, 컨트롤러 mTLS에선 브로커/컨트롤러끼리 서로 인증합니다.

## 4. TLS가 주는 이점

| 기능 | 설명 | PLAINTEXT면 |
| --- | --- | --- |
| 암호화 | 오가는 데이터를 암호화 | 평문 노출(패킷 캡처 시 내용 보임) |
| 서버 인증 | 클라이언트가 "진짜 브로커"임을 인증서로 확인 | 위장 브로커에 붙어도 모름 |
| mTLS(상호 인증) | 브로커도 상대 인증서를 검증 → 신뢰된 상대만 | 아무나 연결 가능 |

## 5. 서버 인증은 어떻게 도나 (예시)

전제: 브로커 keystore에 `SAN=ip:10.0.0.11`, 클라이언트 truststore에 CA.

```text
 클라이언트 ── 10.0.0.11:9094 접속 ──► 브로커(kafka1)
            ◄── 자기 인증서 제시 ──── (CN=kafka1, SAN=ip:10.0.0.11, 발급자 CN=kafka-ca)
   검증:
     (a) 이 인증서가 내 truststore의 CA로 서명됐나?
     (b) 인증서 SAN이 접속 주소 10.0.0.11 과 일치하나?
   둘 다 OK → 진짜 브로커 확인, 통신 시작 ✓
```

**클라이언트 설정(서버 인증을 켜는 부분)**

```properties
security.protocol=SASL_SSL
ssl.truststore.location=/etc/kafka/secrets/truststore.jks   # 신뢰할 CA
ssl.truststore.password=CHANGE_ME_TS
ssl.endpoint.identification.algorithm=https                 # SAN(주소) 검증 ON
```

**직접 확인**

```bash
# 브로커가 제시하는 인증서와 검증 결과
openssl s_client -connect 10.0.0.11:9094 -CAfile ca.crt </dev/null
#   Verify return code: 0 (ok)  ← CA 검증 통과

# 인증서 SAN 이 접속 주소와 맞는지
keytool -list -v -keystore kafka1.keystore.jks -storepass CHANGE_ME_KS \
  | grep -A1 "SubjectAlternativeName"
```

**인증이 실패하는(= 위장을 막는) 경우**

- **위장 브로커**: 내 truststore의 CA로 서명 안 된 인증서 제시 → 거부. (CA 개인키가 없으면 신뢰되는 인증서를 못 만듦)
- **SAN 불일치**: 인증서 SAN과 실제 접속 주소가 다름 → hostname verification 실패. (그래서 SAN에 각 노드 실 주소를 정확히 넣음)
- **검증 끄기 위험**: `ssl.endpoint.identification.algorithm=`(빈 값)이면 SAN 검사를 안 해 중간자에 취약 → `https`로 켜둠.

## 6. PLAINTEXT vs SASL_SSL 선택 기준

| 상황 | 선택 |
| --- | --- |
| 주문·결제·개인정보 등 민감 데이터 | **SASL_SSL** |
| 여러 서비스/팀 공유 프로덕션 | **SASL_SSL** |
| 규제·컴플라이언스(전송 암호화 요구) | **SASL_SSL** |
| 망 격리를 100% 보장 못 함 | **SASL_SSL** |
| 완전 격리·신뢰된 내부망, 학습·검증 | PLAINTEXT 가능 |
| 성능 최우선 + 망 자체가 안전 | PLAINTEXT 가능 |

판단축은 **"내부망을 얼마나 신뢰하느냐 + 데이터가 얼마나 민감하냐"** 입니다.

## 7. 비용 (공짜가 아님)

| 비용 | 내용 |
| --- | --- |
| 설정 복잡도 | 사설 CA·keystore·truststore 관리, 만료·갱신, SCRAM 계정 부트스트랩 |
| 성능 | 암복호화 CPU 부담 + **zero-copy 불가**(→ [04-broker-internals](04-broker-internals.md)) → 처리량·지연에 영향 |
| 운영 | 인증서 만료 관리, 시크릿은 Secret Manager로 주입(파일 커밋 금지) |

서버 스펙에서 CPU를 여유 있게 잡는 이유 중 하나가 이 TLS 오버헤드입니다.

## 한 줄 요약

- **TLS** = 도청·위장 방지(암호화 + 서버 인증), **SASL/SCRAM** = 클라이언트 인증, **ACL** = 권한. 세 축이 다른 문제를 푼다.
- 서버 인증 = 브로커가 CA 서명 인증서(SAN=자기 주소)를 제시 → 클라이언트가 truststore의 CA로 검증.
- 민감 데이터·프로덕션·규제 대상이면 SASL_SSL, 완전 격리 내부망·학습용이면 PLAINTEXT.

## 참고한 공식 문서

- [Security Overview](https://kafka.apache.org/documentation/#security)
- [Encryption and Authentication using SSL](https://kafka.apache.org/documentation/#security_ssl)
- [Authentication using SASL/SCRAM](https://kafka.apache.org/documentation/#security_sasl_scram)
- [Authorization and ACLs](https://kafka.apache.org/documentation/#security_authz)
