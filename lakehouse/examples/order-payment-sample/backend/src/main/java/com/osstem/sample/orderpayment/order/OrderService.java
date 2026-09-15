package com.osstem.sample.orderpayment.order;

import com.osstem.sample.orderpayment.config.AppProperties;
import com.osstem.sample.orderpayment.event.EventPublisher;
import com.osstem.sample.orderpayment.event.OrderEvent;
import com.osstem.sample.orderpayment.event.PaymentEvent;
import com.osstem.sample.orderpayment.payment.Payment;
import com.osstem.sample.orderpayment.payment.PaymentRepository;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderService {

    private final OrderRepository orders;
    private final PaymentRepository payments;
    private final EventPublisher publisher;
    private final AppProperties props;

    public OrderService(OrderRepository orders, PaymentRepository payments, EventPublisher publisher, AppProperties props) {
        this.orders = orders;
        this.payments = payments;
        this.publisher = publisher;
        this.props = props;
    }

    @Transactional
    public Order create(String customerId, String itemName, long amount) {
        if (amount <= 0) {
            throw new IllegalArgumentException("금액은 0보다 커야 합니다");
        }
        Order order = new Order("order-" + shortId(), customerId, itemName, amount);
        orders.save(order);
        publisher.publish(OrderEvent.of("ORDER_CREATED", order.getId(), customerId, itemName, amount, props.sourceSystem()));
        return order;
    }

    /** 결제 완료 주문만 취소할 수 있다. 취소 시 ORDER_CANCELLED 와 PAYMENT_REFUNDED 를 함께 발행한다. */
    @Transactional
    public Order cancel(String orderId) {
        Order order = get(orderId);
        if (order.getStatus() != Order.Status.PAID) {
            throw new IllegalStateException("결제 완료 상태의 주문만 취소할 수 있습니다: " + order.getStatus());
        }
        Payment completed = payments.findFirstByOrderIdAndStatus(orderId, Payment.Status.COMPLETED)
                .orElseThrow(() -> new IllegalStateException("완료된 결제가 없습니다"));
        order.markCancelled();
        completed.markRefunded();
        publisher.publish(OrderEvent.of("ORDER_CANCELLED", order.getId(), order.getCustomerId(), order.getItemName(),
                order.getAmount(), props.sourceSystem()));
        publisher.publish(PaymentEvent.of("PAYMENT_REFUNDED", order.getId(), completed.getId(), order.getCustomerId(),
                completed.getAmount(), completed.getMethod().name(), null, props.sourceSystem()));
        return order;
    }

    @Transactional(readOnly = true)
    public Order get(String orderId) {
        return orders.findById(orderId).orElseThrow(() -> new IllegalArgumentException("주문이 없습니다: " + orderId));
    }

    @Transactional(readOnly = true)
    public List<Order> recent() {
        return orders.findTop50ByOrderByCreatedAtDesc();
    }

    static String shortId() {
        return UUID.randomUUID().toString().substring(0, 8);
    }
}
