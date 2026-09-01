package dev.devopsnote.kafkarunner.ledger;

import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

/** 보낸 것/받은 것의 대조표. 모든 판정의 유일한 근거. 스레드 안전. */
public class Ledger {
    private final Set<Long> sentOk = new ConcurrentSkipListSet<>();
    private final Set<Long> sentFail = new ConcurrentSkipListSet<>();
    private final Set<Long> received = new ConcurrentSkipListSet<>();
    private final AtomicLong receivedTotal = new AtomicLong();
    private final AtomicLong failedEver = new AtomicLong(); // 재전송으로 회복돼도 남는 누적 실패 이력

    public void recordSentOk(long seq) { sentOk.add(seq); sentFail.remove(seq); } // 재전송 성공 반영
    public void recordSentFail(long seq) {
        failedEver.incrementAndGet();
        if (!sentOk.contains(seq)) sentFail.add(seq);
    }
    public void recordReceived(long seq) { received.add(seq); receivedTotal.incrementAndGet(); }

    public Set<Long> sentOk() { return new TreeSet<>(sentOk); }
    public Set<Long> sentFail() { return new TreeSet<>(sentFail); }
    public Set<Long> received() { return new TreeSet<>(received); }
    public Set<Long> lostSeqs() { var lost = new TreeSet<>(sentOk); lost.removeAll(received); return lost; }
    public long duplicateCount() { return receivedTotal.get() - received.size(); }
    public long failedEverCount() { return failedEver.get(); }
}
