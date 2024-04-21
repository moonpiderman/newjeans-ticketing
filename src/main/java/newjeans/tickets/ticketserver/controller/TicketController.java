package newjeans.tickets.ticketserver.controller;

import lombok.RequiredArgsConstructor;
import newjeans.tickets.ticketserver.dto.RegisterUserResponse;
import newjeans.tickets.ticketserver.service.UserQueueService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

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
}
