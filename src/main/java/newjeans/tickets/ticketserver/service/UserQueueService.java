package newjeans.tickets.ticketserver.service;

import lombok.RequiredArgsConstructor;
import newjeans.tickets.ticketserver.exception.ApplicationException;
import newjeans.tickets.ticketserver.exception.ErrorCode;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;

@Service
@RequiredArgsConstructor
public class UserQueueService {
    private final ReactiveRedisTemplate<String, String> reactiveRedisTemplate;
    private final String USER_WAIT_KEY = "users:queue:%s:wait";

    public Mono<Long> registerWaitQueue(final String queue, final Long userId) {
        var unixTimeStamp = Instant.now().getEpochSecond();
        return reactiveRedisTemplate.opsForZSet().add(USER_WAIT_KEY.formatted(queue), userId.toString(), unixTimeStamp)
                .filter(i -> i)
                .switchIfEmpty(Mono.error(ErrorCode.QUEUE_ALREADY_REGISTERED_USER.build()))
                .flatMap(i -> reactiveRedisTemplate.opsForZSet().rank(USER_WAIT_KEY.formatted(queue), userId.toString()))
                .map(i -> i >= 0 ? i + 1: i)
                ;
    }
}
