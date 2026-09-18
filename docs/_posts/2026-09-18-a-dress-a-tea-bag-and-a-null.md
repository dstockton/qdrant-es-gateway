---
layout: post
title: "A dress, a tea bag, and a null"
date: 2026-09-18
categories: [field-notes]
---

Real catalogues are less like neat databases and more like drawers everyone in the house has used.

The [H&M catalogue](https://huggingface.co/datasets/Qdrant/hm_ecommerce_products) starts politely enough: “Strap top”, black, white, off-white. Then the variants multiply, identical descriptions return in several colours, and one of them becomes “Strap top (1)”. The product data is quietly telling us an important truth: an item is not just a sentence. It is a sentence, a colour, a department, a picture, and a suspiciously similar sibling.

[Open Food Facts](https://huggingface.co/datasets/openfoodfacts/product-database) is even more honest. It contains millions of products, dozens of languages, community edits, missing categories, unknown food names, incomplete ingredient percentages, and products whose environmental score is essentially shrug emoji-shaped. One tea bag arrives with `ciqual_food_name = unknown`; another has no nutrition data at all. The database does not hide the mess. It puts the mess in a field and carries on.

That is exactly why search gateways need real catalogues, not just rows called `product-000001`. Search has to survive colour variants, multilingual labels, duplicate-ish names, missing values, and the occasional tea bag with an identity crisis.

The useful benchmark question is therefore not “which engine sorted my toy data fastest?” It is: “can a customer still find the black top, the hazelnut spread, and the product whose category is null?”

Usually, that is where the interesting engineering begins.
