package demo.thangcv.entitys;

import lombok.Data;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Data
@Entity
@Table(name = "thread_config")
public class ThreadConfig {
    @Id
    private String id;
    private int value ;
}
