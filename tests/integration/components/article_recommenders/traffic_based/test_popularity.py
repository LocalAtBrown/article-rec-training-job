import pytest
from article_rec_db.models.article import Language
from article_rec_db.models.recommender import RecommendationType

from article_rec_training_job.components.article_recommenders.traffic_based.popularity import (
    BaseRecommender,
)
from article_rec_training_job.shared.types.article_recommenders import Strategy


@pytest.fixture(scope="function")
def base_recommender_english(sa_session_factory_postgres) -> BaseRecommender:
    return BaseRecommender(
        max_recommendations=20,
        allowed_languages={Language.ENGLISH},
        sa_session_factory=sa_session_factory_postgres,
    )


@pytest.fixture(scope="function")
def base_recommender_multilingual(sa_session_factory_postgres) -> BaseRecommender:
    return BaseRecommender(
        max_recommendations=20,
        allowed_languages={Language.ENGLISH, Language.SPANISH},
        sa_session_factory=sa_session_factory_postgres,
    )


def test_base_recommender_english(base_recommender_english, events, write_articles_to_postgres, article_english_in_house):
    recommender, metrics = base_recommender_english.recommend(events)

    assert recommender.strategy == Strategy.POPULARITY
    assert recommender.recommendation_type == RecommendationType.DEFAULT_AKA_NO_SOURCE
    assert len(recommender.recommendations) == 1

    recommendation = recommender.recommendations[0]
    assert recommendation.source_article is None
    assert recommendation.target_article.site == article_english_in_house.site
    assert recommendation.target_article.id_in_site == article_english_in_house.id_in_site
    assert recommendation.target_article.title == article_english_in_house.title
    assert recommendation.target_article.description == article_english_in_house.description
    assert recommendation.target_article.content == article_english_in_house.content
    assert recommendation.target_article.site_published_at == article_english_in_house.site_published_at
    assert recommendation.target_article.site_updated_at == article_english_in_house.site_updated_at
    assert recommendation.target_article.language == article_english_in_house.language == Language.ENGLISH
    assert recommendation.target_article.is_in_house_content is article_english_in_house.is_in_house_content is True
    assert recommendation.score == 5.2  # total engagement time in seconds

    assert metrics.time_taken_to_create_recommendations > 0
    assert metrics.num_recommendations_created == 1
    assert metrics.num_embeddings_created == 0


def test_base_recommender_multilingual(
    base_recommender_multilingual,
    events,
    write_articles_to_postgres,
    article_english_in_house,
    article_not_english_in_house,
):
    recommender, metrics = base_recommender_multilingual.recommend(events)

    assert recommender.strategy == Strategy.POPULARITY
    assert recommender.recommendation_type == RecommendationType.DEFAULT_AKA_NO_SOURCE
    assert len(recommender.recommendations) == 2

    recommendation_first = recommender.recommendations[0]
    assert recommendation_first.source_article is None
    assert recommendation_first.target_article.site == article_not_english_in_house.site
    assert recommendation_first.target_article.id_in_site == article_not_english_in_house.id_in_site
    assert recommendation_first.target_article.title == article_not_english_in_house.title
    assert recommendation_first.target_article.description == article_not_english_in_house.description
    assert recommendation_first.target_article.content == article_not_english_in_house.content
    assert recommendation_first.target_article.site_published_at == article_not_english_in_house.site_published_at
    assert recommendation_first.target_article.site_updated_at == article_not_english_in_house.site_updated_at
    assert recommendation_first.target_article.language == article_not_english_in_house.language == Language.SPANISH
    assert (
        recommendation_first.target_article.is_in_house_content
        is article_not_english_in_house.is_in_house_content
        is True
    )
    assert recommendation_first.score == 6  # higher score

    recommendation_second = recommender.recommendations[1]
    assert recommendation_second.source_article is None
    assert recommendation_second.target_article.site == article_english_in_house.site
    assert recommendation_second.target_article.id_in_site == article_english_in_house.id_in_site
    assert recommendation_second.target_article.title == article_english_in_house.title
    assert recommendation_second.target_article.description == article_english_in_house.description
    assert recommendation_second.target_article.content == article_english_in_house.content
    assert recommendation_second.target_article.site_published_at == article_english_in_house.site_published_at
    assert recommendation_second.target_article.site_updated_at == article_english_in_house.site_updated_at
    assert recommendation_second.target_article.language == article_english_in_house.language == Language.ENGLISH
    assert (
        recommendation_second.target_article.is_in_house_content is article_english_in_house.is_in_house_content is True
    )
    assert recommendation_second.score == 5.2  # lower score

    assert metrics.time_taken_to_create_recommendations > 0
    assert metrics.num_recommendations_created == 2
    assert metrics.num_embeddings_created == 0
