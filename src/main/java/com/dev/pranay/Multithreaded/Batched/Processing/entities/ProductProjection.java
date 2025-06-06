package com.dev.pranay.Multithreaded.Batched.Processing.entities;

import org.springframework.beans.factory.annotation.Value;

public interface ProductProjection {
    Long getId();
    String getName();
    String getCategory();
    Double getPrice();
    boolean getIsOfferApplied();
//    boolean isOfferApplied();
//
//    @Value("#{target.isOfferApplied}")
//    boolean getOfferApplied();

    Double getDiscountPercentage();
    Double getPriceAfterDiscount();
}

/*
When Spring Data JPA sees a repository method that returns an interface projection
(like Page<ProductProjection>), it inspects the getter methods in that interface.
It then constructs a JPQL query (which Hibernate translates to SQL) that selects
only the corresponding properties from the entity.

So, for Page<ProductProjection> findBy(Pageable pageable);,
Spring Data JPA will automatically generate a query similar to:
SELECT p.id AS id, p.name AS name, p.category AS category, ...
FROM Product p
(The AS alias part is important for mapping the results back to the projection interface's methods).
 */
