import marimo

__generated_with = "0.20.2"
app = marimo.App(width="medium")


@app.cell
def imports():
    import dlt
    import ibis
    import marimo as mo

    return dlt, ibis, mo


@app.cell
def load_data(dlt):
    pipeline = dlt.pipeline(pipeline_name='DE_Z_open_library_pipeline', destination='duckdb')
    dataset = pipeline.dataset()
    con = dataset.ibis()
    db_name = pipeline.dataset_name
    return con, db_name


@app.cell
def process_data(con, db_name, ibis):
    # Retrieve the authors nested table
    authors_table = con.table('search__author_name', database=db_name)

    # Use ibis to group by author name, count books, and sort
    top_authors = (
        authors_table
        .group_by(authors_table.value.name("author_name"))
        .agg(book_count=authors_table.count())
        .order_by(ibis.desc("book_count"))
        .limit(20)
    )
    return (top_authors,)


@app.cell
def render_dashboard(mo, top_authors):
    # Execute the ibis query to get the results into memory
    results_df = top_authors.execute()

    dashboard = mo.md(
        f"""
        # Open Library Pipeline Analysis 📚

        This notebook visualizes the top 20 authors found in the `search__author_name` table based on our latest pipeline run.

        ## Top 20 Authors (by Book Count)
        {mo.ui.table(results_df, selection=None)}
        """
    )

    dashboard
    return


if __name__ == "__main__":
    app.run()
