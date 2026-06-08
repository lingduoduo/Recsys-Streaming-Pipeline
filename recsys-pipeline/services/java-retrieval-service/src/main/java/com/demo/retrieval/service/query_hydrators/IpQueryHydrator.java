package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.service.clients.GeoLocationClient;
import com.demo.retrieval.service.ScoredMoviesQuery;
import org.springframework.stereotype.Component;

/**
 * Hydrates ipLocation from the geo-location client.
 *
 * Mirrors the Rust IpQueryHydrator which calls GeoIpLocationClient.fetch(ip_address)
 * to resolve the request-time IP to a geographic location. For MovieLens there is
 * no per-request IP; GeoLocationClient returns the user's ZIP code from their
 * profile as the pre-computed location equivalent.
 */
@Component
public class IpQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {

    private final GeoLocationClient geoLocationClient;

    public IpQueryHydrator(GeoLocationClient geoLocationClient) {
        this.geoLocationClient = geoLocationClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        return new ScoredMoviesQuery(
            query.userId(),
            query.userFeatures().withIpLocation(geoLocationClient.getLocation(query.userId())),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(
            query.userId(),
            query.userFeatures().withIpLocation(hydrated.userFeatures().ipLocation()),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }
}
