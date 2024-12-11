"""Queries for pandas dataframe"""

import streamlit as st
from streamlit_agraph import agraph, Node, Edge, Config

nodes = []
edges = []

color_images = ["https://www.colorhexa.com/c4d6b0.png",
                "https://www.colorhexa.com/477998.png", "https://www.htmlcsscolor.com/preview/gallery/F64740.png",
                "https://color-hex.org/colors/a3333d.png"]
nodes.append(Node(id="Spiderman",
                  label="",
                  size=25,
                  shape="circularImage",
                  image=color_images[0])
             )
nodes.append(Node(id="Captain_Marvel",
                  size=25,
                  shape="circularImage",
                  image="https://www.colorhexa.com/477998.png")
             )
edges.append(Edge(source="Captain_Marvel",
                  label="friend_of",
                  target="Spiderman",
                  image=color_images[1]

                  )
             )

config = Config(width=750,
                height=950,
                directed=True,
                physics=True,
                hierarchical=False,
                # **kwargs
                )

return_value = agraph(nodes=nodes,
                      edges=edges,
                      config=config)
