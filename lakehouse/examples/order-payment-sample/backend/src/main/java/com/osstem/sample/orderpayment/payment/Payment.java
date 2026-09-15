package com.osstem.sample.orderpayment.payment;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.OffsetDateTime;

@Entity
@Table(name = "payments")
public class Payment {

    public enum Status { COMPLETED, FAILED, REFUNDED }

    public enum Method { CARD, BANK, POINT }

    @Id
    private String id;

    @Column(nullable = false)
    private String orderId;

    @Column(nullable = false)
    private long amount;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private Method method;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private Status status;

    private String failureReason;

    /** true 면 결제 이벤트 발행을 일부러 생략한 건 — 3단계 "주문·결제 불일치 후보" 시연용. */
    @Column(nullable = false)
    private boolean eventSkipped;

    @Column(nullable = false)
    private OffsetDateTime createdAt;

    protected Payment() {}

    public Payment(String id, String orderId, long amount, Method method, Status status, String failureReason,
                   boolean eventSkipped) {
        this.id = id;
        this.orderId = orderId;
        this.amount = amount;
        this.method = method;
        this.status = status;
        this.failureReason = failureReason;
        this.eventSkipped = eventSkipped;
        this.createdAt = OffsetDateTime.now();
    }

    public void markRefunded() { this.status = Status.REFUNDED; }

    public String getId() { return id; }
    public String getOrderId() { return orderId; }
    public long getAmount() { return amount; }
    public Method getMethod() { return method; }
    public Status getStatus() { return status; }
    public String getFailureReason() { return failureReason; }
    public boolean isEventSkipped() { return eventSkipped; }
    public OffsetDateTime getCreatedAt() { return createdAt; }
}
