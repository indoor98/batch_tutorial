package com.tutorial.batchchunk.processor;

import com.tutorial.batchchunk.domain.OSProduct;
import com.tutorial.batchchunk.domain.Product;
import org.springframework.batch.item.ItemProcessor;


public class TransformProductItemProcessor implements ItemProcessor<Product, OSProduct> {


    @Override
    public OSProduct process(Product item) throws Exception {
        System.out.println("Transform processor() executed");

        OSProduct osProduct = new OSProduct();
        osProduct.setProductId(item.getProductId());
        osProduct.setProductName(item.getProductName());
        osProduct.setProductCategory(item.getProductCategory());
        osProduct.setProductPrice(item.getProductPrice());
        osProduct.setTaxPercent(item.getProductCategory().equals("sprots Accessories") ? 5 : 18 );
        osProduct.setSku(item.getProductCategory().substring(0, 3)+item.getProductId());
        osProduct.setShippingRate((item.getProductPrice() < 1000) ? 75 : 0);
        return osProduct;
    }
}
