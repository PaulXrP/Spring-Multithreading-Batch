package com.dev.pranay.Multithreaded.Batched.Processing.repositories;

import com.dev.pranay.Multithreaded.Batched.Processing.entities.DeadLetterRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface DeadLetterRecordRepository extends JpaRepository<DeadLetterRecord, Long> {
}
