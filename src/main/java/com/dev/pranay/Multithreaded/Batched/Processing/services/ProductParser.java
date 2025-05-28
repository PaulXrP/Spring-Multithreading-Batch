package com.dev.pranay.Multithreaded.Batched.Processing.services;

import com.dev.pranay.Multithreaded.Batched.Processing.entities.Product;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class ProductParser {

    public List<Product> parseProduct(List<String> lines) {
        List<Product> products = new ArrayList<>();

        for(String line : lines) {
            String[] fields = line.split(",");

            if(fields.length>=7) {
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
        return products;
    }
}
