package newjeans.tickets.ticketserver.controller;

import lombok.RequiredArgsConstructor;
import newjeans.tickets.ticketserver.dto.AllowUserResponse;
import newjeans.tickets.ticketserver.dto.AllowedUserResponse;
import newjeans.tickets.ticketserver.dto.RankNumberResponse;
import newjeans.tickets.ticketserver.dto.RegisterUserResponse;
import newjeans.tickets.ticketserver.service.UserQueueService;
import org.springframework.http.ResponseCookie;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.time.Duration;

@RestController
@RequestMapping("/v1/tickets")
@RequiredArgsConstructor
public class TicketController {
    private final UserQueueService userQueueService;

    @PostMapping("")
    public Mono<RegisterUserResponse> registerTicket(
            @RequestParam(name = "queue", defaultValue = "default") String queue,
            @RequestParam(name = "user_id") Long userId
    ) {
        return userQueueService.registerWaitQueue(queue, userId)
                .map(RegisterUserResponse::new);
    }

    @PostMapping("/allow")
    public Mono<AllowUserResponse> allowUser(
            @RequestParam(name = "queue", defaultValue = "default") String queue,
            @RequestParam(name = "count") Long count
    ) {
        return userQueueService.allowUser(queue, count)
                .map(i -> new AllowUserResponse(i, i));
    }

    @GetMapping("/allowed")
    public Mono<AllowedUserResponse> isAllowedUser(
            @RequestParam(name = "queue", defaultValue = "default") String queue,
            @RequestParam(name = "user_id") Long userId,
            @RequestParam(name = "token") String token
    ) {
        return userQueueService.isAllowedByToken(queue, userId, token).map(AllowedUserResponse::new);
    }

    @GetMapping("/rank")
    public Mono<RankNumberResponse> getRank(
            @RequestParam(name = "queue", defaultValue = "default") String queue,
            @RequestParam(name = "user_id") Long userId
    ) {
        return userQueueService.getRank(queue, userId).map(RankNumberResponse::new);
    }

    @GetMapping("/touch")
    public Mono<?> touch(
            @RequestParam(name = "queue", defaultValue = "default") String queue,
            @RequestParam(name = "user_id") Long userId,
            ServerWebExchange exchange
    ) {
        return Mono.defer(() -> userQueueService.generateToken(queue, userId))
                .map(token -> {
                    exchange.getResponse().addCookie(
                            ResponseCookie
                                    .from("user-queue-%s-token".formatted(queue), token)
                                    .maxAge(Duration.ofSeconds(300L))
                                    .build()
                    );

                    return token;
                });
    }
}
