package com.dev.pranay.Multithreaded.Batched.Processing.services;

import com.dev.pranay.Multithreaded.Batched.Processing.entities.ProductDocument;
import com.dev.pranay.Multithreaded.Batched.Processing.repositories.ProductElasticsearchRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.query.Criteria;
import org.springframework.data.elasticsearch.core.query.CriteriaQuery;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class ProductSearchService {

    private final ProductElasticsearchRepository productElasticsearchRepository;

    private final ElasticsearchOperations elasticsearchOperations; //for more control

    public List<ProductDocument> searchByName(String name) {
        return productElasticsearchRepository.findByNameContainingIgnoreCase(name);
    }

    public List<ProductDocument> searchProducts(String nameQuery, String category, Double minPrice, Double maxPrice, Boolean offerApplied) {
        CriteriaQuery criteriaQuery = new CriteriaQuery(new Criteria());

        if(nameQuery != null && !nameQuery.isEmpty()) {
            // For phrase matching on 'name' or more complex text search:
            // criteriaQuery.addCriteria(Criteria.where("name").matches(nameQuery)); // Example, might need specific analyzer behavior
            // For simple contains (like SQL LIKE %nameQuery%) on a text field
            criteriaQuery.addCriteria(Criteria.where("name").contains(nameQuery));
        }

        if(category!= null && !category.isEmpty()) {
            criteriaQuery.addCriteria(Criteria.where("category").is(category)); // Exact match for keyword
        }

        if(minPrice!= null) {
            criteriaQuery.addCriteria(Criteria.where("Price").greaterThanEqual(minPrice));
        }

        if(maxPrice != null) {
            criteriaQuery.addCriteria(Criteria.where("price").lessThanEqual(maxPrice));
        }

        if(offerApplied != null) {
            criteriaQuery.addCriteria(Criteria.where("isOfferApplied").is(offerApplied));
        }

        SearchHits<ProductDocument> searchHits = elasticsearchOperations.search(criteriaQuery, ProductDocument.class);

        return searchHits
                .stream()
                .map(SearchHit::getContent)
                .collect(Collectors.toList());
    }
}
