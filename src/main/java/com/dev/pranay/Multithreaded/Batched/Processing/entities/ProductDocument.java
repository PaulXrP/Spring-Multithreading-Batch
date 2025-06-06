package com.dev.pranay.Multithreaded.Batched.Processing.entities;

import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;
import org.springframework.scheduling.annotation.Async;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Document(indexName = "products") //This will be the name of your index in Elasticsearch
public class ProductDocument {

    @Id // Marks this field as the document ID in Elasticsearch
    private Long id; // Usually corresponds to the ID in our primary database

    @Field(type = FieldType.Text, analyzer = "standard") // Text for full-text search, standard analyzer
    private String name;

    @Field(type = FieldType.Keyword) // Keyword for exact matches, filtering, and aggregations
    private String category;

    @Field(type = FieldType.Double)
    private Double price;

    @Field(type = FieldType.Boolean)
    private boolean isOfferApplied;

    @Field(type = FieldType.Double)
    private Double discountPercentage;

    @Field(type = FieldType.Double)
    private Double priceAfterDiscount;
}
