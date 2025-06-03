package com.dev.pranay.Multithreaded.Batched.Processing.repositories;

import com.dev.pranay.Multithreaded.Batched.Processing.entities.Product;
import com.dev.pranay.Multithreaded.Batched.Processing.entities.ProductDocument;
import lombok.RequiredArgsConstructor;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ProductElasticsearchRepository extends ElasticsearchRepository<ProductDocument, Long> {
      List<ProductDocument> findByNameContainingIgnoreCase(String name);
      List<ProductDocument> findByCategoryAndPriceBetween(String category, double minPrice, double maxPrice);
      List<ProductDocument> findByIsOfferAppliedTrue();
}
