package com.example.javatrader.repository;

import com.example.javatrader.model.TickerData;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TickerDataRepository extends JpaRepository<TickerData, Long> {
}
