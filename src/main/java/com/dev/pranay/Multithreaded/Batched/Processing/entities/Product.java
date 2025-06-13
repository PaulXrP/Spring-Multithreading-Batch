package com.dev.pranay.Multithreaded.Batched.Processing.entities;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "products")
@AllArgsConstructor
@NoArgsConstructor
@Data
public class Product {

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
