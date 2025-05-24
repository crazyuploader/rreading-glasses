package hardcover

import (
	"context"

	"github.com/Khan/genqlient/graphql"
)

func SearchAuthor(ctx context.Context, client graphql.Client, query string) (*SearchResponse, error) {
	queryType := "author"
	fields := "name,name_personal,alternate_names"
	weights := "3,2,1"
	sort := "_text_match:desc,books_count:desc"
	return Search(ctx, client, query, fields, weights, queryType, sort)
}

func SearchBook(ctx context.Context, client graphql.Client, query string) (*SearchResponse, error) {
	queryType := "book"
	fields := ""
	weights := ""
	sort := ""
	return Search(ctx, client, query, fields, weights, queryType, sort)
}
