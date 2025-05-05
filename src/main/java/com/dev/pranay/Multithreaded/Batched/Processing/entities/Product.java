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
    private double price;
    @Column(name = "isOfferApplied")
    private boolean isOfferApplied;
    @Column(name = "discountPercentage")
    private double discountPercentage;
    @Column(name = "priceAfterDiscount")
    private double priceAfterDiscount;
}
