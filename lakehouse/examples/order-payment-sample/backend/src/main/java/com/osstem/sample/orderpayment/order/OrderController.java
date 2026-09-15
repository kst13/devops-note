package com.osstem.sample.orderpayment.order;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
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
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderService service;

    public OrderController(OrderService service) {
        this.service = service;
    }

    public record CreateRequest(@NotBlank String customerId, @NotBlank String itemName, @Min(1) long amount) {}

    public record OrderView(String id, String customerId, String itemName, long amount, String status,
                            String createdAt, String updatedAt) {
        static OrderView from(Order o) {
            return new OrderView(o.getId(), o.getCustomerId(), o.getItemName(), o.getAmount(), o.getStatus().name(),
                    o.getCreatedAt().toString(), o.getUpdatedAt() == null ? null : o.getUpdatedAt().toString());
        }
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public OrderView create(@Valid @RequestBody CreateRequest req) {
        return OrderView.from(service.create(req.customerId(), req.itemName(), req.amount()));
    }

    @GetMapping
    public List<OrderView> recent() {
        return service.recent().stream().map(OrderView::from).toList();
    }

    @GetMapping("/{orderId}")
    public OrderView get(@PathVariable String orderId) {
        return OrderView.from(service.get(orderId));
    }

    @PostMapping("/{orderId}/cancel")
    public OrderView cancel(@PathVariable String orderId) {
        return OrderView.from(service.cancel(orderId));
    }
}
