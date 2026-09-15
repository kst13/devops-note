package com.osstem.sample.orderpayment.order;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.OffsetDateTime;

/** 운영 DB 역할(H2). 주문의 현재 상태는 여기가 기준이고, Lakehouse 는 이벤트 이력이다. */
@Entity
@Table(name = "orders")
public class Order {

    public enum Status { CREATED, PAID, PAYMENT_FAILED, CANCELLED }

    @Id
    private String id;

    @Column(nullable = false)
    private String customerId;

    @Column(nullable = false)
    private String itemName;

    @Column(nullable = false)
    private long amount;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private Status status;

    @Column(nullable = false)
    private OffsetDateTime createdAt;

    private OffsetDateTime updatedAt;

    protected Order() {}

    public Order(String id, String customerId, String itemName, long amount) {
        this.id = id;
        this.customerId = customerId;
        this.itemName = itemName;
        this.amount = amount;
        this.status = Status.CREATED;
        this.createdAt = OffsetDateTime.now();
        this.updatedAt = this.createdAt;
    }

    public void markPaid() { transition(Status.PAID); }
    public void markPaymentFailed() { transition(Status.PAYMENT_FAILED); }
    public void markCancelled() { transition(Status.CANCELLED); }

    private void transition(Status next) {
        this.status = next;
        this.updatedAt = OffsetDateTime.now();
    }

    public String getId() { return id; }
    public String getCustomerId() { return customerId; }
    public String getItemName() { return itemName; }
    public long getAmount() { return amount; }
    public Status getStatus() { return status; }
    public OffsetDateTime getCreatedAt() { return createdAt; }
    public OffsetDateTime getUpdatedAt() { return updatedAt; }
}
