def give_fake_id(entities):
    fake_id = 0
    for entity in entities:
        if not entity.id or entity.id.endswith(('Q', 'QNone')):
            entity.id = f"http://www.wikidata.org/entity/fake{fake_id}"
            fake_id += 1