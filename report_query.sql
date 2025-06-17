-- KPI CARDS 
-- total products 
select
	count(distinct fl.item_id) as total_products
from fact_listing fl
left join ebay_categories c on fl.category_id = c.category_id
left join dim_condition cond on fl.condition_id = cond.condition_id
left join dim_date d on fl.created_date_id = d.date_id
where
	c.category_name = ''
    and cond.condition_name = ''
    and d.year = '';

-- total sellers
select
	count(distinct fl.seller_id) as total_sellers
from fact_listing fl
left join ebay_categories c on fl.category_id = c.category_id
left join dim_condition cond on fl.condition_id = cond.condition_id
left join dim_date d on fl.created_date_id = d.date_id
where
	c.category_name = ''
    and cond.condition_name = ''
    and d.year = '';

-- top seller by volume
select
	s.username as seller_name,
    count(distinct fl.item_id) as total_products
from fact_listing fl
left join dim_seller s on fl.seller_id = s.seller_id
left join ebay_categories c on fl.category_id = c.category_id
left join dim_condition cond on fl.condition_id = cond.condition_id
left join dim_date d on fl.created_date_id = d.date_id
where
	c.category_name = ''
    and cond.condition_name = ''
    and d.year = ''
group by s.username
order by total_products desc
limit 1;


-- yearly growth
select
	d.year,
	count(distinct fl.seller_id) as total_sellers,
    count(distinct fl.item_id) as total_products
from fact_listing fl
left join dim_seller s on fl.seller_id = s.seller_id
left join ebay_categories c on fl.category_id = c.category_id
left join dim_condition cond on fl.condition_id = cond.condition_id
left join dim_date d on fl.created_date_id = d.date_id
where
	c.category_name = ''
    and cond.condition_name = ''
    and d.year = ''
group by d.year
order by d.year desc;

-- price distribution
select
  case
    when fl.price < 10 then 'Under $10'
    when fl.price < 25 then '$10 - $24'
    when fl.price < 50 then '$25 - $49'
    when fl.price < 100 then '$50 - $99'
    when fl.price < 250 then '$100 - $249'
    when fl.price < 500 then '$250 - $499'
    when fl.price < 1000 then '$500 - $999'
    else 'Over $1000'
  end as price_range,
  count(distinct fl.item_id) as total_listings
from fact_listing fl
left join ebay_categories c on fl.category_id = c.category_id
left join dim_condition cond on fl.condition_id = cond.condition_id
left join dim_date d on fl.created_date_id = d.date_id
where fl.price is not null
	and d.year = '2025'
group by price_range
order by 
  case 
    when price_range = 'Under $10' then 1
    when price_range = '$10 - $24' then 2
    when price_range = '$25 - $49' then 3
    when price_range = '$50 - $99' then 4
    when price_range = '$100 - $249' then 5
    when price_range = '$250 - $499' then 6
    when price_range = '$500 - $999' then 7
    when price_range = 'Over $1000' then 8
  end;

-- top 5 sellers
select
    s.username as seller_name,
    count(distinct fl.item_id) as total_products
from fact_listing fl
left join dim_seller s on fl.seller_id = s.seller_id
left join ebay_categories c on fl.category_id = c.category_id
left join dim_condition cond on fl.condition_id = cond.condition_id
left join dim_date d on fl.created_date_id = d.date_id
group by s.username
order by total_products desc
limit 5;

-- top 5 products
select
    i.title as product_name,
    count(distinct fl.item_id) as total_products
from fact_listing fl
left join dim_item i on fl.item_id = i.item_id
left join ebay_categories c on fl.category_id = c.category_id
left join dim_condition cond on fl.condition_id = cond.condition_id
left join dim_date d on fl.created_date_id = d.date_id
group by i.title
order by total_products desc
limit 5;

-- seller segmentation by quality
with seller_stats as (
    select
        s.seller_id,
        count(distinct fl.item_id) as listing_count,
        avg(s.feedback_score) as avg_feedback_score
    from fact_listing fl
    left join dim_seller s on fl.seller_id = s.seller_id
    left join ebay_categories c on fl.category_id = c.category_id
	left join dim_condition cond on fl.condition_id = cond.condition_id
	left join dim_date d on fl.created_date_id = d.date_id
    where
		c.category_name = 'Acupuncture'
    group by s.seller_id
),
segmented_sellers as (
	select
		seller_id,
		listing_count,
		avg_feedback_score,
		case
			when listing_count >= 300 and avg_feedback_score >= 100000 then 'Trusted Power Sellers'
			when listing_count >= 300 and avg_feedback_score < 100000 then 'Risky Volume Sellers'
			when listing_count < 300 and avg_feedback_score >= 100000 then 'Niche Trusted Sellers'
			else 'Irrelevant Noise'
		end as quality_segment
	from seller_stats
)
select
	quality_segment,
    count(*) as total_sellers,
    sum(listing_count) as total_products,
    round(avg(avg_feedback_score), 2) as avg_feedback_score
from segmented_sellers
group by quality_segment
order by total_sellers desc;
















