library(shiny)
library(httr)

# Define UI for dataset viewer app ----
ui <- fluidPage(

    titlePanel("Simple UI client"),

    sidebarLayout(
        # Side bar panel for SQL Wrapper connection params
        sidebarPanel(
            width=2,
            "SQL Wrapper Connection",
            fluidRow(
                column(10,
                       textInput("sql_server_url",
                                 h6("URL"),
                                 value = "http://127.0.0.1")
                       )
            ),
            fluidRow(
                column(10,
                       textInput("sql_server_port",
                                 h6("port"),
                                 value = "8000")
                       )
            ),
            fluidRow(
                column(10,
                       submitButton("Submit")
                       )
            )
        ),

        # Main panel for displaying outputs
        mainPanel(
            "query params",
            fluidRow(
                column(3,
                       selectInput("table_name",
                                   h6("Table Name"),
                                   choices = list("empty_list"),
                                   selected = 1)
                       )
            ),
            fluidRow(
                column(10,
                       submitButton("Submit")
                       )
            ),
            fluidRow(
                column(3,
                       "number of rows",
                       verbatimTextOutput("value")
                       )
            )
        )
    )
)

server <- function(input, output, session) {
    # update row count on table_name dropdown
    output$value <- renderText({
        url <- input$sql_server_url
        port <- input$sql_server_port

        rv <- POST(url=paste(url, port, sep=':'), path="get_table_count", body=list(table_name=input$table_name), encode="json")
        content(rv)$count
    })

    # update on server connection params
    observe({
        url <- input$sql_server_url
        port <- input$sql_server_port

        rv <- GET(url=paste(url, port, sep=':'), path="get_table_names")
        table_name_list = content(rv)$table_names

        updateSelectInput(session, "table_name",
                          choices = table_name_list,
                          selected = table_name_list[1]
                          )
    })
}

shinyApp(ui, server)
