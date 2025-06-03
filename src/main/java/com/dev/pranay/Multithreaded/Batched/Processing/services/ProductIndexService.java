package com.dev.pranay.Multithreaded.Batched.Processing.services;

import com.dev.pranay.Multithreaded.Batched.Processing.entities.Product;
import com.dev.pranay.Multithreaded.Batched.Processing.entities.ProductDocument;
import com.dev.pranay.Multithreaded.Batched.Processing.repositories.ProductElasticsearchRepository;
import com.dev.pranay.Multithreaded.Batched.Processing.repositories.ProductRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class ProductIndexService {

    private final ProductRepository productRepository;

    private final ProductElasticsearchRepository productElasticsearchRepository;

    // Method to map JPA entity to Elasticsearch document
    private ProductDocument mapToDocument(Product product) {
         return new ProductDocument(
                 product.getId(),
                 product.getName(),
                 product.getCategory(),
                 product.getPrice(),
                 product.isOfferApplied(),
                 product.getDiscountPercentage(),
                 product.getPriceAfterDiscount()
         );
    }

    public void indexAllProducts() {
        // For very large datasets, process in pages/batches to avoid OutOfMemoryError
        int pageSize = 1000;
        Page<Product> productPage;
        int pageNum = 0;

        do {
            productPage = productRepository.findAll(PageRequest.of(pageNum++, pageSize));
            List<ProductDocument> documents = productPage.getContent().stream()
                    .map(this::mapToDocument)
                    .collect(Collectors.toList());

            if(!documents.isEmpty()) {
                productElasticsearchRepository.saveAll(documents); //Bulk save to ES
                System.out.println("Indexed " + documents.size() + " products from page " + (pageNum-1));
            }
        } while (productPage.hasNext());
        System.out.println("Finished indexing all products.");
    }

    // Index a single product (useful for ongoing sync)
    public void indexProduct(Product product) {
        productElasticsearchRepository.save(mapToDocument(product));
    }

    // Delete a product from index (if it's deleted from DB)
    public void deleteProductFromIndex(Long productId) {
       productElasticsearchRepository.deleteById(productId);
    }
}
