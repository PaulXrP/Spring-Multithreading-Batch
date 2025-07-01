package com.dev.pranay.Multithreaded.Batched.Processing.batch.readers;

import com.dev.pranay.Multithreaded.Batched.Processing.entities.Product;
import com.dev.pranay.Multithreaded.Batched.Processing.repositories.ProductRepository;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A thread-safe, keyset-based ItemReader for processing entities in parallel.
 * This reader is designed to be resilient against concurrent modifications of the underlying data.
 * It fetches pages based on the last processed ID rather than a numeric offset.
 */

public class SynchronizedKeysetItemReader implements ItemReader<Product> {

    private final ProductRepository productRepository;
    private final int pageSize;

    private Long lastId = 0L;
    private final List<Product> buffer = new CopyOnWriteArrayList<>();

    public SynchronizedKeysetItemReader(ProductRepository productRepository, int pageSize) {
        this.productRepository = productRepository;
        this.pageSize = pageSize;
    }

    @Override
    public synchronized Product read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        // If the buffer is empty, it's time to fetch the next page from the database.
        // The synchronized keyword ensures that only one thread can execute this block at a time,
        // preventing multiple threads from fetching the same page.
        if(buffer.isEmpty()) {
            List<Product> newItems = productRepository.findTopWithIdGreaterThan(lastId, pageSize);

            if(newItems.isEmpty()) {
                // No more items to process, the job is done.
                return null;
            }

            // Add the new items to our thread-safe buffer.
            buffer.addAll(newItems);

            // Update the lastId to the ID of the last item in the newly fetched page.
            // This will be used for the next database query.
            lastId = newItems.get(newItems.size() - 1).getId();
        }

        // Threads can safely remove items from the buffer concurrently.
        // If the buffer becomes empty, the next thread to call read() will trigger a new database fetch.
        if(buffer.isEmpty()) {
            return null; // Should not happen if newItems was not empty, but good for safety.
        }

        return buffer.remove(0);
    }
}
