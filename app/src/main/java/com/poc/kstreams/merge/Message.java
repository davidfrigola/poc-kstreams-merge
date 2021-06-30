package com.poc.kstreams.merge;

import lombok.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
@Builder
public class Message {

    private String id;

    private String origin;

    private String value1; // origin_1

    private String value2; // origin_2

    private String value3; // origin_2
}
