package com.gelq;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CartInfo {
    private String sensorId;//信号灯ID
    private Integer count;//通过该信号灯的车的数量
}
