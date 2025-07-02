package com.dev.pranay.Multithreaded.Batched.Processing.entities;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Entity
@Table(name = "products")
@AllArgsConstructor
@NoArgsConstructor
@Data

// Implementing Serializable to the class definition
public class Product implements Serializable {

    // It's a best practice to add a version UID for serializable classes.
    // This helps with versioning if you change the class structure later.
    private static final long serialVersionUID = 1L;

    @Id
    private Long id;
    private String name;
    private String category;
    private Double price;
    @Column(name = "is_offer_applied")
    private boolean isOfferApplied;
    @Column(name = "discount_percentage")
    private Double discountPercentage;
    @Column(name = "price_after_discount")
    private Double priceAfterDiscount;

    /**
     * This flag is the key to making the batch job idempotent and restartable.
     * We will only stream records where this is false, and we'll set it to true
     * within the same transaction that we apply the discount.
     * Ensure the database column for this defaults to 'false'.
     */

    private boolean postProcessed = false;
}

/**
 * The serialVersionUID is a private static final field.
 * The Java Persistence API (JPA) and its implementation (Hibernate) are designed to
 * map an entity's non-static, non-transient instance fields to database columns.
 *
 * Because serialVersionUID is static, the persistence framework completely ignores it
 * during the process of creating or updating the database schema.
 * It is used exclusively by Java's internal serialization mechanism to ensure that
 * a serialized object can be correctly deserialized later.
 */
