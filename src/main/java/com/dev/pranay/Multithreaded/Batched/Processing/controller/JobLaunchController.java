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
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * A REST controller to provide endpoints for launching various batch jobs.
 */

@RestController
@RequestMapping("/api/jobs")
@Slf4j
public class JobLaunchController {

    private final JobLauncher jobLauncher;

    //We inject both jobs and use @Qualifier to distinguish them
    @Qualifier("discountProcessingJob")
    private final Job singleNodeJob;

    @Qualifier("remoteChunkingManagerJob")
    private final Job managerJob;

    /**
     * Using an explicit constructor with @Qualifier annotations to resolve ambiguity.
     * This tells Spring exactly which Job bean to inject for each parameter.
     *
     * @param jobLauncher The default job launcher.
     * @param singleNodeJob The bean named "discountProcessingJob".
     * @param managerJob The bean named "remoteChunkingManagerJob".
     */
    public JobLaunchController(
            JobLauncher jobLauncher,
            @Qualifier("discountProcessingJob") Job singleNodeJob,
            @Qualifier("remoteChunkingManagerJob") Job managerJob
    ) {
        this.jobLauncher = jobLauncher;
        this.singleNodeJob = singleNodeJob;
        this.managerJob = managerJob;
    }

    @PostMapping("/start-single-node-job")
    public ResponseEntity<String> startSingleNodeJob() {
        log.info("Received request to start the single-node discount processing job.");
        return launchJob(singleNodeJob, "singleNodeJob");
    }

    @PostMapping("/start-manager-job")
    public ResponseEntity<String> startManagerJob() {
        log.info("Received request to start the Remote Chunking Manager job.");
        return launchJob(managerJob, "managerJob");
    }

    private ResponseEntity<String> launchJob(Job job, String jobName) {
       try {
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("startTime", System.currentTimeMillis())
                    .toJobParameters();

            jobLauncher.run(job, jobParameters);
           return ResponseEntity.ok("Job '" + jobName + "' started successfully.");
       } catch (Exception e) {
           log.error("Failed to start the job '{}'", jobName, e);
           return ResponseEntity.internalServerError().body("Failed to start the job: " + e.getMessage());
       }
    }

    @PostMapping("/start-discount-job")
    public ResponseEntity<String> startDiscountJob() {
        log.info("Received request to start the discount processing job.");
        try {
            // Using System.currentTimeMillis() ensures that each job instance has unique parameters,
            // allowing it to be run more than once.
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("startTime", System.currentTimeMillis())
                    .toJobParameters();

            jobLauncher.run(singleNodeJob, jobParameters);
            return ResponseEntity.ok("Discount processing job started successfully.");
        } catch (Exception e) {
            log.error("Failed to start the discount processing job", e);
            return ResponseEntity.internalServerError().body("Failed to start job: " + e.getMessage());
        }
    }
}
