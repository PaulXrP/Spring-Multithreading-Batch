package com.dev.pranay.Multithreaded.Batched.Processing.config;

import org.springframework.context.annotation.Configuration;

@Configuration
public class PaginationConfig {

    /**
     * The number of records to fetch from the database in a single page (a single chunk).
     * This directly corresponds to the batch size that will be processed by each thread.
     */

    public static final int PAGE_SIZE =  1000;
}
