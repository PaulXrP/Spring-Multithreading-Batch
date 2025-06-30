package com.dev.pranay.Multithreaded.Batched.Processing.batch.listener;


import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;

/**
 * A listener that provides hooks before a job starts and after it completes.
 * This is essential for logging the final outcome and triggering notifications.
 */

@Component
@Slf4j
public class JobCompletionNotificationListener implements JobExecutionListener {
    @Override
    public void beforeJob(JobExecution jobExecution) {
        log.info("!!! JOB STARTING: {} !!!", jobExecution.getJobInstance().getJobName());
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        if(jobExecution.getStatus() == BatchStatus.COMPLETED) {
            log.info("!!! JOB FINISHED: {} completed successfully. !!!", jobExecution.getJobInstance().getJobName());
        } else if (jobExecution.getStatus() == BatchStatus.FAILED) {
            log.info("!!! JOB FAILED: {} failed with status {}. !!!",
                    jobExecution.getJobInstance().getJobName(), jobExecution.getStatus());
        }

    }
}
