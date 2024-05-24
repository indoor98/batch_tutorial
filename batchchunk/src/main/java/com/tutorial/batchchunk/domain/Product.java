package com.tutorial.batchchunk.domain;


import lombok.*;

@Getter
@Setter
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class Product {

    private Integer productId;
    private String productName;
    private String productCategory;
    private Integer productPrice;
}
