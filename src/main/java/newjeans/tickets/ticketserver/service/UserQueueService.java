package newjeans.tickets.ticketserver.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import newjeans.tickets.ticketserver.exception.ErrorCode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.zset.Tuple;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;

import java.time.Instant;
import java.util.Objects;

@Service
@RequiredArgsConstructor
@Slf4j
public class UserQueueService {
    private final ReactiveRedisTemplate<String, String> reactiveRedisTemplate;
    private final String USER_WAIT_KEY = "users:queue:%s:wait";
    private final String USER_WAIT_KEY_FOR_SCAN = "users:queue:*:wait";
    private final String USER_PROCEED_KEY = "users:queue:%s:proceed";

    @Value("${scheduler.enabled}")
    private Boolean scheduling = false;

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

    public Mono<Long> getRank(final String queue, final Long userId) {
        return reactiveRedisTemplate.opsForZSet().rank(USER_WAIT_KEY.formatted(queue), userId.toString())
                .defaultIfEmpty(-1L)
                .map(rank -> rank >= 0 ? rank + 1: rank);
    }

    @Scheduled(initialDelay = 5000, fixedRate = 3000)
    public void scheduleAllowUser() {
        if (!scheduling) {
            log.info("PASSED SCHEUDLING");
            return;
        }

        log.info("called scheduling ...! ");

        var maxAllowUserCount = 3L;

        reactiveRedisTemplate.scan(ScanOptions.scanOptions()
                .match(USER_WAIT_KEY_FOR_SCAN)
                .count(100)
                .build())
                .map(key -> key.split(":")[2])
                .flatMap(queue -> allowUser(queue, maxAllowUserCount).map(allowed -> Tuples.of(queue, allowed)))
                .doOnNext(tuple -> log.info("Tried %d and allowed %s members of %s queue.".formatted(maxAllowUserCount, tuple.getT1(), tuple.getT2())))
                .subscribe();
    }
}
