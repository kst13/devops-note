package com.osstem.sample.orderpayment.payment;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/payments")
public class PaymentController {

    private final PaymentService service;

    public PaymentController(PaymentService service) {
        this.service = service;
    }

    public record PayRequest(@NotBlank String orderId, @NotNull Payment.Method method, boolean skipEvent) {}

    public record PaymentView(String id, String orderId, long amount, String method, String status,
                              String failureReason, boolean eventSkipped, String createdAt) {
        static PaymentView from(Payment p) {
            return new PaymentView(p.getId(), p.getOrderId(), p.getAmount(), p.getMethod().name(), p.getStatus().name(),
                    p.getFailureReason(), p.isEventSkipped(), p.getCreatedAt().toString());
        }
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public PaymentView pay(@Valid @RequestBody PayRequest req) {
        return PaymentView.from(service.pay(req.orderId(), req.method(), req.skipEvent()));
    }

    @GetMapping("/by-order/{orderId}")
    public List<PaymentView> byOrder(@PathVariable String orderId) {
        return service.byOrder(orderId).stream().map(PaymentView::from).toList();
    }
}
