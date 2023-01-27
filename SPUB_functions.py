def give_fake_id(entities):
    fake_id = 0
    for entity in entities:
        # if pl.id.endswith(('Q', 'QNone')):
        if not entity.id:
            entity.id = f"http://www.wikidata.org/entity/fake{fake_id}"
            fake_id += 1