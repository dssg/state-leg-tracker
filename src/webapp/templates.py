import urllib.parse


def load_css() -> str:
    """ Return all css styles. """
    common_tag_css = """
                display: inline-flex;
                align-items: center;
                justify-content: center;
                padding: .15rem .40rem;
                position: relative;
                text-decoration: none;
                font-size: 95%;
                border-radius: 5px;
                margin-right: .5rem;
                margin-top: .4rem;
                margin-bottom: .5rem;
    """
    return f"""
        <style>
            #tags {{
                {common_tag_css}
                color: rgb(88, 88, 88);
                border-width: 0px;
                background-color: rgb(240, 242, 246);
            }}
            #tags:hover {{
                color: black;
                box-shadow: 0px 5px 10px 0px rgba(0,0,0,0.2);
            }}
            #active-tag {{
                {common_tag_css}
                color: rgb(246, 51, 102);
                border-width: 1px;
                border-style: solid;
                border-color: rgb(246, 51, 102);
            }}
            #active-tag:hover {{
                color: black;
                border-color: black;
                background-color: rgb(240, 242, 246);
                box-shadow: 0px 5px 10px 0px rgba(0,0,0,0.2);
            }}
        </style>
    """

def number_of_results(total_hits: int, duration: float) -> str:
    """ HTML scripts to display number of results and duration. """
    return f"""
        <div style="color:grey;font-size:95%;">
            {total_hits} results ({duration:.2f} seconds)
        </div><br>
    """

def search_result(
    i: int, 
    url: str, 
    title: str, 
    description: str, 
    bill_id,
    bill_number:str, 
    state: str, 
    session_name: str,
    # highlights: str,
    # author: str, 
    # length: str, 
    **kwargs) -> str:
    """ HTML scripts to display search results. """
    # Find a way to add padding between the two divs
    return f"""
            <div style="pading: 510px">
                {' '}
            </div>

            <div style="font-size:110%; border-radius: 10px; border: none; padding: 10px;">
                <a href="?bill_id={bill_id}" target="_blank" rel="noopener noreferrer">
                    {title}
                </a> 
                <br/>
                {bill_number} | {state} | {session_name}
                <div style="color:grey; font-size:95%;">
                    {description[:500] + '...' if len(description) > 500 else description}
                </div>
            </div>
            
        

    """

    # {title}
    # {description}
                # {url[:90] + '...' if len(url) > 100 else url}
                # <br>
    # <div style="font-size:95%;">
    #         <div style="color:grey;font-size:95%;">
    #             {url[:90] + '...' if len(url) > 100 else url}
    #         </div>
    #         <div style="float:left;font-style:italic;">
    #             {author} ·&nbsp;
    #         </div>
    #         <div style="color:grey;float:left;">
    #             {length} ...
    #         </div>
    #         {highlights}
    #     </div>

def tag_boxes(search: str, tags: list, active_tag: str) -> str:
    """ HTML scripts to render tag boxes. """
    html = ''
    search = urllib.parse.quote(search)
    for tag in tags:
        if tag != active_tag:
            html += f"""
            <a id="tags" href="?search={search}&tags={tag}">
                {tag.replace('-', ' ')}
            </a>
            """
        else:
            html += f"""
            <a id="active-tag" href="?search={search}">
                {tag.replace('-', ' ')}
            </a>
            """

    html += '<br><br>'
    return html


def pagination(total_pages: int, search: str, current_page: int) -> str:
    """ HTML scripts to render pagination buttons. """
    # search words and tags
    params = f'?search={urllib.parse.quote(search)}'

    # avoid invalid page number (<=0)
    if (current_page - 5) > 0:
        start_from = current_page - 5
    else:
        start_from = 1

    hrefs = []
    if current_page != 1:
        hrefs += [
            f'<a href="{params}&page={1}">&lt&ltFirst</a>',
            f'<a href="{params}&page={current_page - 1}">&ltPrevious</a>',
        ]
        
    for i in range(start_from, min(total_pages + 1, start_from + 10)):
        if i == current_page:
            hrefs.append(f'{current_page}')
        else:
            hrefs.append(f'<a href="{params}&page={i}">{i}</a>')

    if current_page != total_pages:
        hrefs.append(f'<a href="{params}&page={current_page + 1}">Next&gt</a>')

    return '<div>' + '&emsp;'.join(hrefs) + '</div>'