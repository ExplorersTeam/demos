package org.exp.demos.utils;

/**
 * Operations to message queue.
 *
 * @author ChenJintong
 *
 */
public interface MQHandler {
    void produce(String message);

    void consume();

}
