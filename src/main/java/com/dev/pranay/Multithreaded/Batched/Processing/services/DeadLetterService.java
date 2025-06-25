package com.dev.pranay.Multithreaded.Batched.Processing.services;

import com.dev.pranay.Multithreaded.Batched.Processing.entities.DeadLetterRecord;
import com.dev.pranay.Multithreaded.Batched.Processing.repositories.DeadLetterRecordRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class DeadLetterService {

    private final DeadLetterRecordRepository recordRepository;

    /**
     * Persists a list of failed entity IDs to the dead-letter table.
     * This method runs in its own NEW transaction to ensure that even if the calling
     * transaction is rolled back, the record of the failure is still saved. This is critical for
     * creating a reliable audit trail of failures.
     *
     * @param jobName The name of the job that produced the failure.
     * @param entityIds The primary keys of the entities that failed.
     * @param reason The exception message or reason for failure.
     */

    @Transactional
    public void recordFailures(String jobName, List<Long> entityIds, String reason) {
        try {
            List<DeadLetterRecord> deadLetterRecords = entityIds.stream()
                    .map(entityId -> new DeadLetterRecord(jobName, entityId, reason))
                    .collect(Collectors.toList());

            recordRepository.saveAll(deadLetterRecords);
            log.info("Successfully recorded {} failed entities to the DLQ for job '{}'.", deadLetterRecords.size(), jobName);
        } catch (Exception e) {
            // This is a critical failure, as we cannot even record the original failure.
            // Log with high priority.
            log.error("CRITICAL: Could not write to Dead Letter Queue for job '{}'. Reason: {}", jobName, e.getMessage(), e);
        }
    }
}
