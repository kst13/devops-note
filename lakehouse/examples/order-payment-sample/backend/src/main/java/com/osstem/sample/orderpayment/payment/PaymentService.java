package com.osstem.sample.orderpayment.payment;

import com.osstem.sample.orderpayment.config.AppProperties;
import com.osstem.sample.orderpayment.event.EventPublisher;
import com.osstem.sample.orderpayment.event.PaymentEvent;
import com.osstem.sample.orderpayment.order.Order;
import com.osstem.sample.orderpayment.order.OrderService;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class PaymentService {

    private final PaymentRepository payments;
    private final OrderService orderService;
    private final FakePaymentGateway gateway;
    private final EventPublisher publisher;
    private final AppProperties props;

    public PaymentService(PaymentRepository payments, OrderService orderService, FakePaymentGateway gateway,
                          EventPublisher publisher, AppProperties props) {
        this.payments = payments;
        this.orderService = orderService;
        this.gateway = gateway;
        this.publisher = publisher;
        this.props = props;
    }

    /**
     * 결제 흐름: PAYMENT_REQUESTED 발행 → 가짜 PG 판정 → H2 저장 → COMPLETED/FAILED 발행.
     * skipEvent 가 true 면 H2 에는 완료로 남기고 결제 이벤트를 발행하지 않는다 (불일치 후보 시연).
     */
    @Transactional
    public Payment pay(String orderId, Payment.Method method, boolean skipEvent) {
        Order order = orderService.get(orderId);
        if (order.getStatus() == Order.Status.PAID) {
            throw new IllegalStateException("이미 결제된 주문입니다");
        }
        if (order.getStatus() == Order.Status.CANCELLED) {
            throw new IllegalStateException("취소된 주문입니다");
        }

        String paymentId = "pay-" + UUID.randomUUID().toString().substring(0, 8);
        String customer = order.getCustomerId();
        long amount = order.getAmount();
        String source = props.sourceSystem();

        if (!skipEvent) {
            publisher.publish(PaymentEvent.of("PAYMENT_REQUESTED", orderId, paymentId, customer, amount,
                    method.name(), null, source));
        }

        Optional<String> failure = gateway.tryCharge(amount, method);
        Payment payment;
        if (failure.isPresent()) {
            payment = new Payment(paymentId, orderId, amount, method, Payment.Status.FAILED, failure.get(), skipEvent);
            order.markPaymentFailed();
            if (!skipEvent) {
                publisher.publish(PaymentEvent.of("PAYMENT_FAILED", orderId, paymentId, customer, amount,
                        method.name(), failure.get(), source));
            }
        } else {
            payment = new Payment(paymentId, orderId, amount, method, Payment.Status.COMPLETED, null, skipEvent);
            order.markPaid();
            if (!skipEvent) {
                publisher.publish(PaymentEvent.of("PAYMENT_COMPLETED", orderId, paymentId, customer, amount,
                        method.name(), null, source));
            }
        }
        return payments.save(payment);
    }

    @Transactional(readOnly = true)
    public List<Payment> byOrder(String orderId) {
        return payments.findByOrderIdOrderByCreatedAtDesc(orderId);
    }
}
