package com.bubbletea.payment.infrastructure.config;

import java.util.concurrent.Executor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * @TimeLimiter가 Toss 호출을 감싸려면 CompletableFuture로 실행돼야 해서 전용 풀을 둔다.
 * TimeLimiter가 타임아웃되면 "호출한 쪽"(confirm-execution-group 컨슈머 스레드)은 바로 풀려나지만,
 * 이 풀 안에서 실제로 Toss 소켓을 읽고 있는 스레드 자체는 (Feign이 인터럽트에 바로 반응하지 않으면)
 * 응답이 올 때까지 계속 점유돼있을 수 있다 - 그래서 컨슈머 풀(20개)보다 여유 있게 잡아서,
 * 이 풀이 느려지는 게 컨슈머 풀 고갈로 이어지지 않게 한다.
 */
@Configuration
public class TossClientConfig {

    @Bean
    public Executor tossCallExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(30);
        executor.setMaxPoolSize(50);
        executor.setQueueCapacity(50);
        executor.setThreadNamePrefix("toss-call-");
        executor.initialize();
        return executor;
    }
}
