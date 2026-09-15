package com.osstem.sample.orderpayment.payment;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PaymentRepository extends JpaRepository<Payment, String> {
    List<Payment> findByOrderIdOrderByCreatedAtDesc(String orderId);
    Optional<Payment> findFirstByOrderIdAndStatus(String orderId, Payment.Status status);
}
