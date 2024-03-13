import streamlit as st
import time
from st_concurrency_limiter import concurrency_limiter


@concurrency_limiter(max_concurrency=2)
def heavy_computation():
    st.write("Heavy computation")
    progress_text = "Operation in progress. Please wait."
    my_bar = st.progress(0, text=progress_text)

    for percent_complete in range(100):
        time.sleep(0.15)
        my_bar.progress(percent_complete + 1, text=progress_text)
    st.write("END OF Heavy computation")
    st.balloons()
    return 42


my_button = st.button("Run heavy computation")

if my_button:
    heavy_computation()
