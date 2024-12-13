""""dfsdf"""

from os import environ as ENV
import pandas as pd
from dotenv import load_dotenv
import streamlit as st
from pytrends.request import TrendReq
from queries import get_related_words
from psycopg2.extensions import cursor as curs
from streamlit_agraph import agraph, Node, Edge, Config
from Home import get_connection

load_dotenv()
pd.set_option('display.precision', 2)
API_ENDPOINT = ENV["API_ENDPOINT"]
COLOUR_PALETTE = ['#C4D6B0', '#477998', '#F64740', '#A3333D']
COLOUR_IMAGES = ["https://www.colorhexa.com/c4d6b0.png",
                 "https://www.colorhexa.com/477998.png"]


def display_center_message() -> None:
    """Display a grey message in the center of the screen for new users"""
    st.markdown(
        """
        <div style="
            display: flex;
            justify-content: center;
            align-items: center;
            height: 80vh;
            color: grey;
            font-size: 20px;
        ">
        Please log in on the home page to see related terms for a trend.
        </div>
        """,
        unsafe_allow_html=True
    )


def initialize_trend_request() -> TrendReq:
    """Initialize and return a TrendReq object."""
    return TrendReq()


def fetch_suggestions(pytrend: TrendReq, keyword: str) -> list[dict]:
    """Fetch and print suggestions for a given keyword."""
    return pytrend.suggestions(keyword=keyword)


def network_graph(keyword: str, cursor: curs) -> agraph:
    """Make a network graph for all the related terms of a given keyword"""
    nodes = []
    edges = []
    related_terms = {}

    result = get_related_words(keyword, cursor)
    related_terms[keyword] = [row.get('related_term') for row in result]

    added_node_ids = set()

    if keyword not in added_node_ids:
        nodes.append(Node(
            id=keyword,
            label=keyword,
            size=30,
            shape="circularImage",
            image="https://color-hex.org/colors/a3333d.png"
        ))
        added_node_ids.add(keyword)

    for idx, related_word in enumerate(related_terms[keyword]):
        if related_word not in added_node_ids:
            nodes.append(Node(
                id=related_word,
                label=related_word,
                size=30,
                shape="circularImage",
                image=COLOUR_IMAGES[idx % 2]
            ))
            added_node_ids.add(related_word)

        edges.append(Edge(
            source=keyword,
            label='',
            target=related_word
        ))

    config = Config(
        width=750,
        height=600,
        directed=True,
        physics=True,
        hierarchical=False,
    )

    clicked_node = agraph(nodes=nodes, edges=edges, config=config)
    return clicked_node


def network_graph_2(keyword: str, cursor: curs) -> agraph:
    """Make a network graph for all the related terms of a given keyword"""
    nodes = []
    edges = []
    related_terms = {}
    pytrend = initialize_trend_request()
    result = fetch_suggestions(pytrend, keyword)

    related_terms[keyword] = [row.get('title') for row in result]

    added_node_ids = set()

    if keyword not in added_node_ids:
        nodes.append(Node(
            id=keyword,
            label=keyword,
            size=30,
            shape="circularImage",
            image="https://color-hex.org/colors/a3333d.png"
        ))
        added_node_ids.add(keyword)

    for idx, related_word in enumerate(related_terms[keyword]):
        if related_word not in added_node_ids:
            nodes.append(Node(
                id=related_word,
                label=related_word,
                size=30,
                shape="circularImage",
                image=COLOUR_IMAGES[idx % 2]
            ))
            added_node_ids.add(related_word)

        edges.append(Edge(
            source=keyword,
            label='',
            target=related_word
        ))

    config = Config(
        width=750,
        height=600,
        directed=True,
        physics=True,
        hierarchical=False,
    )

    clicked_node = agraph(nodes=nodes, edges=edges, config=config)
    return clicked_node


def display_user_page_visuals_networks(selected_keywords: list, cursor: curs):
    """Display network graphs if only one or two keywords are present."""
    if 'clicked_nodes' not in st.session_state:
        st.session_state.clicked_nodes = []

    if len(selected_keywords) == 1:
        text, middle, _ = st.columns([1, 5, 1])
        with text:
            st.write("Explore the related terms: ")
        with middle:
            clicked_node = network_graph(selected_keywords[0], cursor)
            if clicked_node:
                st.session_state.clicked_nodes.append(clicked_node)
                st.rerun()

    elif len(selected_keywords) == 2:
        st.write("Explore the related terms:")
        graph1, _, graph2 = st.columns([7, 1, 7])
        with graph1:
            clicked_node1 = network_graph(selected_keywords[0], cursor)
            # if clicked_node1:
            # st.session_state.clicked_nodes.append(clicked_node1)
            # st.rerun()
        with graph2:
            clicked_node2 = network_graph(selected_keywords[1], cursor)
            # if clicked_node2:
            # st.session_state.clicked_nodes.append(clicked_node2)
            # st.rerun()


def display_user_page_visuals_networks_2(selected_keywords: list, cursor: curs) -> None:
    """Display network graphs if only one or 2 keywords are present."""
    if 'graph_rerun_triggered' not in st.session_state:
        st.session_state.graph_rerun_triggered = False
    if len(selected_keywords) == 1:
        text, middle, _ = st.columns([1, 5, 1])
        with text:
            st.write("Explore the related terms: ")
        with middle:
            clicked_node = network_graph_2(selected_keywords[0], cursor)
            if clicked_node:
                st.session_state.clicked_nodes.append(clicked_node)
                st.session_state.graph_rerun_triggered = True
                st.rerun()

    elif len(selected_keywords) == 2:
        st.write("Explore the related terms:")
        graph1, _, graph2 = st.columns([7, 1, 7])
        with graph1:
            clicked_node1 = network_graph(selected_keywords[0], cursor)
            # if clicked_node1:
            # st.session_state.clicked_nodes.append(clicked_node1)
            # st.rerun()
        with graph2:
            clicked_node2 = network_graph(selected_keywords[1], cursor)
            # if clicked_node2:
            # st.session_state.clicked_nodes.append(clicked_node2)
            # st.rerun()


if __name__ == "__main__":
    _, cursor = get_connection()
    if "user_id" in st.session_state:
        if len(st.session_state.get('clicked_nodes', [])) != 0:
            display_user_page_visuals_networks_2([st.session_state.get(
                'clicked_nodes', [])[-1]], cursor)
        else:
            display_user_page_visuals_networks(
                st.session_state.get("selected_keywords"), cursor)
    else:
        display_center_message()
