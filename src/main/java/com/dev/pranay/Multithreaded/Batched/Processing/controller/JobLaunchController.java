package com.dev.pranay.Multithreaded.Batched.Processing.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/jobs")
@Slf4j
@RequiredArgsConstructor
public class JobLaunchController {

    private final JobLauncher jobLauncher;
    private final Job discountProcessingJob;

    @PostMapping("/start-discount-job")
    public ResponseEntity<String> startDiscountJob() {
        log.info("Received request to start the discount processing job.");
        try {
            // Using System.currentTimeMillis() ensures that each job instance has unique parameters,
            // allowing it to be run more than once.
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("startTime", System.currentTimeMillis())
                    .toJobParameters();

            jobLauncher.run(discountProcessingJob, jobParameters);
            return ResponseEntity.ok("Discount processing job started successfully.");
        } catch (Exception e) {
            log.error("Failed to start the discount processing job", e);
            return ResponseEntity.internalServerError().body("Failed to start job: " + e.getMessage());
        }
    }
}
