package com.demo.retrieval.service;

public interface QueryHydrator<T> {
    T hydrate(T query);

    T update(T query, T hydrated);
}
