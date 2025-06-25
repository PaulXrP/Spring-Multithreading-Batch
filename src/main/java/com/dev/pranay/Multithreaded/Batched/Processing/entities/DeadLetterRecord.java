package com.dev.pranay.Multithreaded.Batched.Processing.entities;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;
import java.time.Instant;

@Entity
@Data
@AllArgsConstructor
@Table(name = "dead_letter_records")
public class DeadLetterRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, updatable = false)
    private String jobName;

    @Column(nullable = false, updatable = false)
    private Long failedEntityId;

    @Column(length = 1000, updatable = false)
    private String reason;

    @Column(nullable = false, updatable = false)
    private Timestamp failureTimestamp;

    public DeadLetterRecord(String jobName, Long failedEntityId, String reason) {
        this.jobName = jobName;
        this.failedEntityId = failedEntityId;
        this.reason = reason;
        this.failureTimestamp = Timestamp.from(Instant.now());
    }
}
