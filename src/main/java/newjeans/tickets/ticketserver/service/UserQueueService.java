package newjeans.tickets.ticketserver.service;

import lombok.RequiredArgsConstructor;
import newjeans.tickets.ticketserver.dto.AllowUserResponse;
import newjeans.tickets.ticketserver.exception.ApplicationException;
import newjeans.tickets.ticketserver.exception.ErrorCode;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Objects;

@Service
@RequiredArgsConstructor
public class UserQueueService {
    private final ReactiveRedisTemplate<String, String> reactiveRedisTemplate;
    private final String USER_WAIT_KEY = "users:queue:%s:wait";
    private final String USER_PROCEED_KEY = "users:queue:%s:proceed";

    public Mono<Long> registerWaitQueue(final String queue, final Long userId) {
        var unixTimeStamp = Instant.now().getEpochSecond();
        return reactiveRedisTemplate.opsForZSet().add(USER_WAIT_KEY.formatted(queue), userId.toString(), unixTimeStamp)
                .filter(i -> i)
                .switchIfEmpty(Mono.error(ErrorCode.QUEUE_ALREADY_REGISTERED_USER.build()))
                .flatMap(i -> reactiveRedisTemplate.opsForZSet().rank(USER_WAIT_KEY.formatted(queue), userId.toString()))
                .map(i -> i >= 0 ? i + 1: i)
                ;
    }

    // 진입 가능 상태인지 조회
    // 진입 허용
    public Mono<Long> allowUser(final String queue, final Long count) {
        // 진입허용 단계
        // 1. wait 사용자 제거
        // 2. proceed 사용자 추가

        return reactiveRedisTemplate.opsForZSet().popMin(USER_WAIT_KEY.formatted(queue), count)
                .flatMap(member -> reactiveRedisTemplate.opsForZSet().add(USER_PROCEED_KEY.formatted(queue), Objects.requireNonNull(member.getValue()), Instant.now().getEpochSecond()))
                .count();

    }

    public Mono<Boolean> isAllowed(final String queue, final Long userId) {
        return reactiveRedisTemplate.opsForZSet().rank(USER_PROCEED_KEY.formatted(queue), userId.toString())
                .defaultIfEmpty(-1L)
                .map(rank -> rank >= 0);
    }
}
