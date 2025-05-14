#!/usr/bin/env python3
import sys

from datetime import datetime
import gradio as gr

from base import get_df, base_path, fpost

vmap = {"identical": 4.0, "closely related": 3.0, "distantly related": 2.0, "unrelated": 1.0, "cannot decide": 0.0}

def enrich(n, d):
    context = d[f"context{n}"]
    prange = d[f"pos{n}"]
    (start, end) = (int(i) for i in prange.split(":"))
    return f"Sentence {n}:<br/><p p style='font-size:large'>{context[:start]}<strong>{context[start:end]}</strong>{context[end:]}</p>"

def store(value, comment):
    """identifier1	identifier2	annotator	judgment	comment	lemma	timestamp	identifier1_system	identifier2_system"""
    df = get_df()
    # print(df.shape, file=sys.stderr)
    # print("-"*20, file=sys.stderr)
    row = df.loc[0]
    # print(type(df), df)
    if value:
        identifier1 = row['identifier1']
        identifier2 = row['identifier2']
        lemma = row['lemma']
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        result = [identifier1, identifier2, "web", str(vmap[value]), comment, lemma, timestamp]
        with open(f"{base_path}/{fpost}", "a") as fout:
            fout.write('\t'.join(result))
            fout.write("\n")

    return (
        df,
        enrich(1, row),
        enrich(2, row),
        # nrow['lemma']
    )

gr.set_static_paths(paths=["../corpus/MetaLing"])


with gr.Blocks(css="footer {visibility: hidden} gradio-app {max-width: 800px; margin: 0 auto}") as demo:
    gr_data = gr.Dataframe(get_df(), visible=False)
    gr.HTML("<h2>Annotation negotiation: comparisons with disagreement</h2>")  
    with gr.Row():  
        gr_sent1 = gr.HTML(label="Sentence 1")
        gr_sent2 = gr.HTML(label="Sentence 2")
    gr_radio = gr.Radio(vmap.keys(), label="Please indicate the semantic relatedness of the two uses of the marked words in the sentences above")
    # with gr.Row():
    gr_comment = gr.Text("", label="Optional comment")
    gr_submit = gr.Button("Submit comment")

    inps = [
        gr_radio,
        gr_comment,
    ]
    outs = [
        gr_data,
        gr_sent1,
        gr_sent2,
    ]

    params = {
        "fn": store,
        "inputs": inps,
        "outputs": outs,
    }

    demo.load(**params)
    gr_radio.change(**params).success(lambda: None, js="window.location.reload()")
    gr_submit.click(**params).success(lambda: None, js="window.location.reload()")

# demo.launch(share=True)
demo.launch(server_port=7861, server_name='0.0.0.0', show_api=False)