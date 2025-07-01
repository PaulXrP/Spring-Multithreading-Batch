package com.dev.pranay.Multithreaded.Batched.Processing.repositories;

import com.dev.pranay.Multithreaded.Batched.Processing.entities.Product;
import com.dev.pranay.Multithreaded.Batched.Processing.entities.ProductProjection;
import jakarta.persistence.QueryHint;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Stream;

import static org.hibernate.jpa.HibernateHints.*;

@Repository
public interface ProductRepository extends JpaRepository<Product, Long> {

    @Modifying
    @Query("UPDATE Product p SET p.isOfferApplied = false, p.priceAfterDiscount = p.price, p.discountPercentage = 0")
    void resetAllProductFields();

    @Modifying
    @Query(value = "UPDATE products SET is_offer_applied = false, price_after_discount = price, discount_percentage = 0", nativeQuery = true)
    void resetAllProductFieldsNative();

    @Query("SELECT p.id FROM Product p")
    List<Long> findAllProductIds();

    // For Strategy (Batch with Fetch)
    List<Product> findTop1000By(); // Retrieves top 1000 records (no filter)

    // For Strategy (Pure JPQL Delete)
//    @Modifying
//    @Query("DELETE FROM Product p WHERE p.status = :status")
//    void deleteByStatus(@Param("status") String status);
//
//    // For Strategy (Native SQL)
//    @Modifying
//    @Query(value = "DELETE FROM product WHERE created_at < :cutoff", nativeQuery = true)
//    void deleteOldRecords(@Param("cutoff") LocalDateTime cutoff);

//    @Modifying
//    @Query("DELETE FROM Product p WHERE p.status = 'DISCONTINUED'")
//    void deleteDiscontinued();

    // JPQL Query: Deletes products where 'price' is less than or equal to the specified amount
    @Modifying
    @Transactional // Good practice to annotate modifying queries with @Transactional
    @Query("DELETE FROM Product p WHERE p.price <= :maxPrice")
    int deleteProductsBelowPrice(@Param("maxPrice") double maxPrice);

    @Modifying
    @Transactional
    @Query("DELETE FROM Product p WHERE p.category = :category")
    int deleteByCategory(String category);

//    @Query("SELECT p.id AS id, p.name AS name, p.category AS category, " +
//            "p.price AS price, p.isOfferApplied AS isOfferApplied, " +
//            "p.discountPercentage AS discountPercentage, p.priceAfterDiscount AS priceAfterDiscount " +
//            "FROM Product p")
//    Page<ProductProjection> findAllProjected(Pageable pageable);


    //    // Spring Data JPA will automatically generate the SELECT clause
    //    // based on the ProductProjection interface
    Page<ProductProjection> findAllBy(Pageable pageable); // Returns all entities as projections


    /**
     * Resets the discount-related fields for all products in a single bulk update operation.
     * The @Modifying annotation is crucial as it indicates that this query will change the database state.
     *
     * @return The number of rows affected by the update.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE Product p SET p.isOfferApplied = false, p.priceAfterDiscount = p.price, p.discountPercentage = 0")
    int resetAllProducts();

    /*
     * NEW STREAMING METHOD
     * This method returns a forward-only, read-only stream of products.
     * The database connection remains open while the stream is being processed.
     * It's crucial to wrap the call to this method in a try-with-resources block.
     * @QueryHints are used to instruct Hibernate to optimize for streaming by not caching entities
     * and using a server-side cursor.
     */

     @Query("SELECT p FROM Product p")
     @QueryHints(value = {
             @QueryHint(name = HINT_FETCH_SIZE, value = "2000"), //// Same as our batch size
             @QueryHint(name = HINT_CACHEABLE, value = "false"),
             @QueryHint(name = HINT_READ_ONLY, value = "true")
     })
     Stream<Product> streamAllProducts();

    /**
     * This is the core method for our restartable stream.
     * It efficiently streams only the products that have not yet been processed.
     */
     @Query("SELECT p FROM Product p WHERE p.postProcessed = false")
     @QueryHints(value = {
             @QueryHint(name = HINT_FETCH_SIZE, value = "2000"), // DB fetch size
             @QueryHint(name = HINT_CACHEABLE, value = "false"), // Don't cache results
             @QueryHint(name = HINT_READ_ONLY, value = "true") // Read-only optimization
     })
     Stream<Product> streamUnprocessedProducts();


    /**
     * Finds all products that have not yet been post-processed, returning the results
     * in a paginated format. The {@code postProcessed} flag is crucial for making the
     * job idempotent (safe to re-run).
     *
     * @param pageable The pagination information (page number, size).
     * @return A Page of unprocessed Products.
     */
    @Query("SELECT p FROM Product p WHERE p.postProcessed = false OR p.postProcessed IS NULL")
    Page<Product> findUnprocessed(Pageable pageable);

    // Fetch the next 2000 unprocessed products after the given ID, in ascending order.
    List<Product> findTop2000ByIdGreaterThanOrderByIdAsc(Integer lastProcessedId);

    @Query("SELECT p FROM Product p WHERE p.id > :lastId AND p.postProcessed = false ORDER BY p.id ASC LIMIT :pageSize")
    List<Product> findTopWithIdGreaterThan(Long lastId, int pageSize);









        /*
        In contrast, if this were not set-based:

    If you had to delete records one by one, you'd be looking at a pattern like:

    Java
    // Hypothetical inefficient way
    List<Product> productsToDelete = productRepository.findByCategory("electronics");
    // First network trip for SELECT
    for (Product product : productsToDelete) {
        productRepository.delete(product); // N-many network trips for individual DELETEs
    }
    This would involve:

    One network trip to fetch all products in the category (potentially bringing many objects
    into memory).
    N additional network trips (where N is the number of products) for each
    individual DELETE statement.
    The explicit @Modifying @Query bypasses this inefficient object-by-object deletion
    and leverages the set-based nature of SQL directly, resulting in the single,
    efficient network round-trip for all matching records.
         */
}
