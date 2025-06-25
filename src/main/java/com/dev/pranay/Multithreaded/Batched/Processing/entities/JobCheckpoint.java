package com.dev.pranay.Multithreaded.Batched.Processing.entities;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Data
@AllArgsConstructor
@NoArgsConstructor
public class JobCheckpoint {

    /**
     * The unique name of the job being tracked.
     */
    @Id
    private String jobName;

    /**
     * The last successfully processed page number. This is the checkpoint.
     */
    private int lastProcessedPage;
}
