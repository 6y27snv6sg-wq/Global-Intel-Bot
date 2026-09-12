import unittest

import bot
from news_engine import NewsItem


def make_item(
    title,
    source="مصدر إخباري",
    *,
    region="",
    official=False,
    forced="",
):
    item = NewsItem(
        title=title,
        url="https://example.com/story",
        source=source,
        summary="",
        region=region,
        official=official,
    )
    if forced:
        item._exclusive_topic = forced
    return item


class ResolvedTopicCharacterizationTests(unittest.TestCase):
    """Freeze the current routing contract before refactoring _resolved_topic."""

    def assert_topic(self, expected, item):
        self.assertEqual(
            bot._resolved_topic(item),
            expected,
            msg=(
                f"title={item.title!r} source={item.source!r} "
                f"region={item.region!r} official={item.official!r} "
                f"forced={getattr(item, '_exclusive_topic', '')!r}"
            ),
        )

    def test_economic_institution_wins_over_security_words(self):
        self.assert_topic(
            "econ",
            make_item(
                "البنك يراجع تمويل منظومة دفاع جوي جديدة",
                source="البنك المركزي",
            ),
        )

    def test_official_institution_defaults_to_official_even_with_security_words(self):
        self.assert_topic(
            "forg",
            make_item(
                "عملية عسكرية جديدة ودفاع جوي ضمن التطورات",
                source="وزارة الخارجية",
            ),
        )

    def test_security_institution_non_operational_ceremony_routes_official(self):
        self.assert_topic(
            "forg",
            make_item(
                "وزارة الدفاع تقيم مراسم وتستقبل وفداً في زيارة رسمية",
                source="وزارة الدفاع",
            ),
        )

    def test_security_institution_wins_over_economic_title(self):
        self.assert_topic(
            "secu",
            make_item(
                "وزارة الدفاع تعلن ميزانية واستثمارات جديدة",
                source="وزارة الدفاع",
            ),
        )

    def test_forced_security_without_concrete_security_evidence_is_not_trusted(self):
        self.assert_topic(
            "wrld",
            make_item(
                "مباحثات دولية حول التعاون المشترك",
                source="مصدر عام",
                forced="secu",
            ),
        )

    def test_forced_security_with_concrete_security_evidence_is_security(self):
        self.assert_topic(
            "secu",
            make_item(
                "بدء مناورات عسكرية مشتركة اليوم",
                source="مصدر عام",
                forced="secu",
            ),
        )

    def test_explicit_official_language_routes_official(self):
        self.assert_topic(
            "forg",
            make_item(
                "بيان رسمي بشأن التطورات الإقليمية",
                source="مصدر عام",
            ),
        )

    def test_strong_economic_signal_routes_economy(self):
        self.assert_topic(
            "econ",
            make_item(
                "البنك يعلن قرار أسعار الفائدة والتضخم",
                source="مصدر عام",
            ),
        )

    def test_strong_security_signal_routes_security(self):
        self.assert_topic(
            "secu",
            make_item(
                "انطلاق تمرين عسكري يتضمن دفاع جوي وقوات خاصة",
                source="مصدر عام",
            ),
        )

    def test_native_official_item_defaults_to_official(self):
        self.assert_topic(
            "forg",
            make_item(
                "اجتماع دوري لمناقشة عدد من الملفات",
                source="جهة رسمية",
                official=True,
            ),
        )

    def test_general_middle_east_story_routes_gulf(self):
        self.assert_topic(
            "gulf",
            make_item(
                "السعودية تستضيف مؤتمراً إقليمياً",
                source="مصدر عام",
                region="الشرق الأوسط",
            ),
        )

    def test_general_world_story_routes_world(self):
        self.assert_topic(
            "wrld",
            make_item(
                "الصين واليابان تبحثان التعاون العلمي",
                source="مصدر عام",
                region="آسيا",
            ),
        )

    def test_forced_urgent_is_excluded_from_specialist_topics_when_no_other_signal(self):
        self.assert_topic(
            "",
            make_item(
                "تطور عاجل قيد المتابعة",
                source="مصدر عام",
                forced="urg",
            ),
        )


if __name__ == "__main__":
    unittest.main()
