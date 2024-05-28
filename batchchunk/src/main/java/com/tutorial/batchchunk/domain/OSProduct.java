package com.tutorial.batchchunk.domain;


import lombok.*;

@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class OSProduct extends Product {

    private Integer taxPercent;
    private String sku;

    private Integer shippingRate;


}
