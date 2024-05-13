import streamlit as st

# Titre de l'application
st.title("Saisie du Nom")

# Champ de saisie pour le nom
nom_utilisateur = st.text_input("Entrez votre nom")

print(nom_utilisateur)