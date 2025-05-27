package com.dev.pranay.Multithreaded.Batched.Processing.services;

import com.dev.pranay.Multithreaded.Batched.Processing.entities.Product;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

@RequiredArgsConstructor
public class ProductBatchInserterForAsync implements Runnable{

    private final List<String> csvLines;
    private final Consumer<List<Product>> persistFunction;

    @Override
    public void run() {
        List<Product> products = new ArrayList<>();
        for(String line : csvLines) {
            String[] fields = line.split(",");
            if(fields.length >= 3) {
                Product product = new Product();
                product.setId(Long.parseLong(fields[0].trim()));
                product.setName(fields[1].trim());
                product.setCategory(fields[2].trim());
                product.setPrice(Double.parseDouble(fields[3].trim()));
                product.setOfferApplied(Boolean.parseBoolean(fields[4].trim()));
                product.setDiscountPercentage(Double.parseDouble(fields[5].trim()));
                product.setPriceAfterDiscount(Double.parseDouble(fields[6].trim()));
                products.add(product);
            }
        }

        // Call Spring-managed persistence logic
       persistFunction.accept(products);
    }
}

//This lets the actual DB logic stay within a Spring-managed, transactional context.
