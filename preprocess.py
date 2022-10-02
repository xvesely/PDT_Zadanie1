from utils import (
    copy_data_to_table, log_time, not_duplicate, exists, 
    make_string_valid, exists_same_row
)


def prepare_authors(obj):
    if exists(obj, "id", is_id=True) == False:
        return None
    try:
        obj["id"] = int(obj["id"])
    except:
        return None

    nullable_string_attributes = [
        "name",
        "username",
        "description"
    ]
    for str_attrb in nullable_string_attributes:
        if exists(obj, str_attrb) == False:
            obj[str_attrb] = None
        else:
            obj[str_attrb] = make_string_valid(obj[str_attrb])

    obj["name"] = obj["name"][:255]
    obj["username"] = obj["username"][:255]

    public_metrics = [
        "followers_count",
        "following_count"
        "tweet_count"
        "listed_count"
    ]
    if exists(obj, "public_metrics") == False:
        obj["public_metrics"] = {}

        for m in public_metrics:
            obj["public_metrics"][m] = None
    else:
        for m in public_metrics:
            if exists(obj["public_metrics"], m) == False:
                obj["public_metrics"][m] = None
            else:
                try:
                    obj["public_metrics"][m] = int(obj["public_metrics"][m])
                except:
                    obj["public_metrics"][m] = None

    table_row = [
        obj["id"],
        obj["name"],
        obj["username"],
        obj["description"],
        obj["public_metrics"]["followers_count"],
        obj["public_metrics"]["following_count"],
        obj["public_metrics"]["tweet_count"],
        obj["public_metrics"]["listed_count"],
    ]
    return table_row


def check_conversation_validity(obj):
    attrs_to_check = [
        "id",
        "author_id",
        "created_at"
    ]
    for attr in attrs_to_check:
        if exists(obj, attr, is_id=True) == False:
            return False
        if attr == "id" or attr == "author_id":
            try:
                x = int(obj[attr])
            except:
                return False

    return True


def prepare_conversation(obj, prepare_other_models=False):
    if exists(obj, "id", is_id=True) == False:
        return None
    try:
        obj["id"] = int(obj["id"])
    except:
        return None

    if exists(obj, "author_id", is_id=True) == False:
        return None
    try:
        obj["author_id"] = int(obj["author_id"])
    except:
        return None

    possibly_string_attributes = [
        "text",
        "lang",
        "source"
    ]

    for attr in possibly_string_attributes:
        if exists(obj, attr) == False:
            obj[attr] = ""
        obj[attr] = make_string_valid(obj[attr])

    obj["lang"] = obj["lang"][:3]

    try:
        obj["possibly_sensitive"] = bool(obj["possibly_sensitive"])
    except:
        obj["possibly_sensitive"] = False

    public_metrics = [
        "retweet_count",
        "reply_count",
        "like_count",
        "quote_count",
    ]

    if exists(obj, "public_metrics") == False:
        obj["public_metrics"] = {}

        for m in public_metrics:
            obj["public_metrics"][m] = None
    else:
        for m in public_metrics:
            if exists(obj["public_metrics"], m) == False:
                obj["public_metrics"][m] = None
            else:
                try:
                    obj["public_metrics"][m] = int(obj["public_metrics"][m])
                except:
                    obj["public_metrics"][m] = None

    if exists(obj, "created_at") == False:
        return None
    obj["created_at"] = make_string_valid(obj["created_at"])

    conversation_row = [
        obj["id"],
        obj["author_id"],
        obj["text"],
        obj["possibly_sensitive"],
        obj["lang"],
        obj["source"],
        obj["public_metrics"]["retweet_count"],
        obj["public_metrics"]["reply_count"],
        obj["public_metrics"]["like_count"],
        obj["public_metrics"]["quote_count"],
        obj["created_at"]
    ]

    if prepare_other_models == False:
        return conversation_row

    hashtag_rows = prepare_hashtags(obj)
    annotation_rows = prepare_annotations(obj)
    link_rows = prepare_links(obj)
    context_domain_rows, context_entity_rows, context_annotation_rows = prepare_context_annotations(obj)
    conversation_reference_rows = prepare_conversation_references(obj)

    return (
        conversation_row,
        hashtag_rows,
        annotation_rows,
        link_rows,
        context_domain_rows,
        context_entity_rows,
        context_annotation_rows,
        conversation_reference_rows
    )


def prepare_hashtags(conversation):
    if exists(conversation, "entities") == False:
        return None

    entities = conversation["entities"]
    if exists(entities, "hashtags"):
        hashtags_arr = []

        for hashtag in entities["hashtags"]:
            if exists(hashtag, "tag") and len(hashtag["tag"]) > 0:
                hashtags_arr.append(make_string_valid(hashtag["tag"]))

        hashtags_arr = list(set(hashtags_arr))
        return [[tag] for tag in hashtags_arr]
    return None


def prepare_annotations(conversation):
    if exists(conversation, "entities") == False:
        return None

    entities = conversation["entities"]
    if exists(entities, "annotations"):
        annotations_arr = []

        for annot in entities["annotations"]:
            attr_names = [
                "normalized_text",
                "type",
                "probability"
            ]
            attr_states = [exists(annot, attr) for attr in attr_names]
            if sum(attr_states) != len(attr_names):
                continue

            values_to_add = ([conversation["id"]] +
                             [make_string_valid(annot[attr]) for attr in attr_names])

            if exists_same_row(annotations_arr, values_to_add) == False:
                annotations_arr.append(values_to_add)

        return annotations_arr
    return None


def prepare_links(conversation):
    if exists(conversation, "entities") == False:
        return None

    entities = conversation["entities"]
    if exists(entities, "urls"):
        link_arr = []

        for link in entities["urls"]:
            attr_names = [
                "expanded_url",
                "title",
                "description"
            ]
            values = [conversation["id"]]

            if exists(link, "expanded_url") == False:
                continue

            for attr in attr_names:
                if exists(link, attr) == False:
                    values.append(None)
                else:
                    values.append(make_string_valid(link[attr]))

            if len(values[1]) > 2048:
                continue
        
            if exists_same_row(link_arr, values) == False:
                link_arr.append(values)

        return link_arr
    return None


def prepare_context_annotations(conversation):
    if exists(conversation, "context_annotations") == False:
        return None, None, None

    context_annotations = conversation["context_annotations"]
    domain_arr = []
    entity_arr = []
    domain_entity_rels = []
    domain_entity_rels_ids = {}

    for context in context_annotations:
        main_attr = [
            "domain",
            "entity"
        ]
        attr_states = [exists(context, attr) for attr in main_attr]

        if sum(attr_states) != len(main_attr):
            continue

        two_objects = []
        for attr in main_attr:
            obj = context[attr]

            if exists(obj, "id", is_id=True) == False or exists(obj, "name") == False:
                break
            try:
                obj["id"] = int(obj["id"])
            except:
                break
    
            obj["name"] = make_string_valid(obj["name"])

            if exists(obj, "description"):
                obj["description"] = make_string_valid(obj["description"])
            else:
                obj["description"] = None

            two_objects.append([
                obj["id"],
                obj["name"][:255],
                obj["description"]
            ])

        if len(two_objects) != 2:
            continue

        domain, entity = two_objects
        domain_arr.append(domain)
        entity_arr.append(entity)

        new_domain_entity_rel = [
            conversation["id"],
            domain[0],
            entity[0]
        ]
        new_domain_entity_rel_id = f"{conversation['id']}-{domain[0]}-{entity[0]}" 
        if not_duplicate(domain_entity_rels_ids, new_domain_entity_rel_id, cast_to_int=False):
            domain_entity_rels.append(new_domain_entity_rel)

    if len(domain_arr) > 0:
        return domain_arr, entity_arr, domain_entity_rels
    return None, None, None


def prepare_conversation_references(conversation):
    if exists(conversation, "referenced_tweets") == False:
        return None

    references_arr = []
    references = conversation["referenced_tweets"]

    for ref in references:
        if exists(ref, "id", is_id=True) == False or exists(ref, "type") == False:
            continue

        values = [conversation["id"]]
        try:
            values.append(int(ref["id"]))
        except:
            continue
        values.append(make_string_valid(ref["type"])[:20])

        if exists_same_row(references_arr, values) == False:
            references_arr.append(values)

    if len(references_arr) > 0:
        return references_arr
    return None
