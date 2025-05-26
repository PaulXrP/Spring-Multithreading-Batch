package com.dev.pranay.Multithreaded.Batched.Processing.repositories;

import com.dev.pranay.Multithreaded.Batched.Processing.entities.Product;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface ProductRepository extends JpaRepository<Product, Long> {

    @Modifying
    @Query("UPDATE Product p SET p.isOfferApplied = false, p.priceAfterDiscount = p.price, p.discountPercentage = 0")
    void resetAllProductFields();

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
