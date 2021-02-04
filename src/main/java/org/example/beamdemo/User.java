package org.example.beamdemo;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
public class User implements Serializable {

    Integer id;
    String firstName;
    String lastName;
    String email;
    String phone;

}
